/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package worker

import (
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const tabletDiffHTML = `
<!DOCTYPE html>
<head>
  <title>Tablet Diff Action</title>
</head>
<body>
  <h1>Tablet Diff Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
      {{range $i, $si := .Tablets}}
        <li><a href="/Diffs/TabletDiff?destination={{$si.Destination}}&source={{$si.Source}}">{{$si.Destination}} in {{$si.Keyspace}}/{{$si.Shard}}</a></li>
      {{end}}
    {{end}}
</body>
`

const tabletDiffHTML2 = `
<!DOCTYPE html>
<head>
  <title>Tablet Diff Action</title>
</head>
<body>
  <h1>Tablet Diff Action</h1>
  <p>Tablet source: {{.Source}}/{{.Shard}}</p>
  <p>Tablet destination: {{.Destination}}/{{.Shard}}</p>
  <form action="/Diffs/TabletDiff" method="post">
      <LABEL for="excludeTables">Exclude Tables: </LABEL>
        <INPUT type="text" id="excludeTables" name="excludeTables" value=""></BR>
      <LABEL for="parallelDiffsCount">Number of tables to diff in parallel: </LABEL>
        <INPUT type="text" id="parallelDiffsCount" name="parallelDiffsCount" value="{{.DefaultParallelDiffsCount}}"></BR>
      <INPUT type="hidden" name="destination" value="{{.Destination}}"/>
      <INPUT type="hidden" name="source" value="{{.Source}}"/>
      <INPUT type="submit" name="submit" value="Table Diff"/>
  </form>
  </body>
`

var tabletDiffTemplate = mustParseTemplate("tabletDiff", tabletDiffHTML)
var tabletDiffTemplate2 = mustParseTemplate("tabletDiff2", tabletDiffHTML2)

func commandTabletDiff(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of tables to exclude")
	parallelDiffsCount := subFlags.Int("parallel_diffs_count", defaultParallelDiffsCount, "number of tables to diff in parallel")
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 2 {
		subFlags.Usage()
		return nil, fmt.Errorf("command TabletDiff requires <fromTablet> <toTablet>")
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	return NewTabletDiffWorker(wr, subFlags.Arg(0), subFlags.Arg(1), int64(*parallelDiffsCount), excludeTableArray, &wrangler.Cleaner{}), nil
}

// shardSources returns all the shards that are SourceShards of at least one other shard.
func tableSources(ctx context.Context, wr *wrangler.Wrangler) ([]map[string]string, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	keyspaces, err := wr.TopoServer().GetKeyspaces(shortCtx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to get list of keyspaces: %v", err)
	}

	wg := sync.WaitGroup{}
	mu := sync.Mutex{} // protects sourceShards
	var sourceShards []map[string]string
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
			shards, err := wr.TopoServer().GetShardNames(shortCtx, keyspace)
			cancel()
			if err != nil {
				rec.RecordError(fmt.Errorf("failed to get list of shards for keyspace '%v': %v", keyspace, err))
				return
			}
			for _, shard := range shards {
				wg.Add(1)
				go func(keyspace, shard string) {
					defer wg.Done()

					shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
					tablets, err := wr.TopoServer().FindAllTabletAliasesInShard(shortCtx, keyspace, shard)
					cancel()
					if err != nil {
						rec.RecordError(fmt.Errorf("failed to get tablet alias information for shard '%v': %v", topoproto.KeyspaceShardString(keyspace, shard), err))
						return
					}

					// We want to remember a replica for each cell in this keyspace/shard
					type tabletAndCell struct {
						tablet string
						cell   string
					}

					sourceTablets := make(map[string]string)
					var tabletAliases []tabletAndCell

					for _, tabletAlias := range tablets {
						// Go through all tablets, taking note of replicas
						shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
						tablet, err := wr.TopoServer().GetTablet(shortCtx, tabletAlias)
						cancel()
						if err != nil {
							rec.RecordError(fmt.Errorf("failed to get tablet information for tablet'%v': %v", tabletAlias.String(), err))
							return
						}

						if tablet.Type == topodatapb.TabletType_REPLICA {
							sourceTablets[tablet.Alias.Cell] = tablet.AliasString()
						} else if tablet.Type != topodatapb.TabletType_MASTER && tablet.Type != topodatapb.TabletType_REPLICA {
							tabletAliases = append(tabletAliases, tabletAndCell{tablet.AliasString(), tablet.Alias.Cell})
						}
					}

					for _, tabletAlias := range tabletAliases {
						if source, ok := sourceTablets[tabletAlias.cell]; ok {
							mu.Lock() // Protect sourceShards from concurrent appending

							sourceShards = append(sourceShards,
								map[string]string{
									"Keyspace":    keyspace,
									"Shard":       shard,
									"Destination": tabletAlias.tablet,
									"Source":      source,
								})
							mu.Unlock()
						}
					}

				}(keyspace, shard)
			}
		}(keyspace)
	}
	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	if len(sourceShards) == 0 {
		return nil, fmt.Errorf("unable to find any tablets to diff against")
	}
	return sourceShards, nil
}

func interactiveTabletDiff(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {

	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse form: %s", err)
	}
	destination := r.FormValue("destination")

	if destination == "" {
		// display the list of possible tables to chose from
		result := make(map[string]interface{})
		tables, err := tableSources(ctx, wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Tablets"] = tables
		}
		return nil, tabletDiffTemplate, result, nil
	}

	submitButtonValue := r.FormValue("submit")
	if submitButtonValue == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Destination"] = destination
		result["Source"] = r.FormValue("source")
		result["DefaultParallelDiffsCount"] = fmt.Sprintf("%v", defaultParallelDiffsCount)
		return nil, tabletDiffTemplate2, result, nil
	}

	// Process input form.
	excludeTables := r.FormValue("excludeTables")
	srcTablet := r.FormValue("source")
	dstTablet := r.FormValue("destination")
	var excludeTableArray []string
	if excludeTables != "" {
		excludeTableArray = strings.Split(excludeTables, ",")
	}
	parallelDiffsCountStr := r.FormValue("parallelDiffsCount")
	parallelDiffsCount, err := strconv.ParseInt(parallelDiffsCountStr, 0, 64)

	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse parallelDiffsCount: %s", err)
	}

	// start the diff job
	wrk := NewTabletDiffWorker(wr, srcTablet, dstTablet, parallelDiffsCount, excludeTableArray, &wrangler.Cleaner{})
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Diffs", Command{"TabletDiff",
		commandTabletDiff, interactiveTabletDiff,
		"<sourceTablet> <destinationTablet>",
		"Does a diff between two tablets, making sure that they are in sync"})
}
