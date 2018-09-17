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

package tabletmanager

import (
	"errors"
	"time"

	"github.com/golang/glog"

	"golang.org/x/net/context"
)

// LockTables will lock all tables with read locks, effectively pausing replication while the lock is held (idempotent)
func (agent *ActionAgent) LockTables(ctx context.Context) error {
	// get a connection
	agent.mutex.Lock()
	defer agent.mutex.Unlock()

	if agent._lockTablesConnection != nil {
		// tables are already locked, bail out
		return errors.New("tables already locked on this tablet")
	}

	conn, err := agent.MysqlDaemon.GetDbaConnection()
	if err != nil {
		return err
	}
	_, err = conn.ExecuteFetch("FLUSH TABLES WITH READ LOCK", 0, false)
	if err != nil {
		return err
	}

	agent._lockTablesConnection = conn
	agent._lockTablesTimer = time.AfterFunc(time.Minute, func() {
		// Here we'll sleep until the timeout time has elapsed.
		// If the table locks have not been released yet, we'll release them here
		agent.mutex.Lock()
		defer agent.mutex.Unlock()

		// We need the mutex locked before we check this field
		if agent._lockTablesConnection == conn {
			glog.Errorf("table lock timed out and released the lock - something went wrong")
			err = agent.unlockTablesHoldingMutex()
			if err != nil {
				glog.Errorf("failed to unlock tables: %v", err)
			}
		}
	})

	return nil
}

// UnlockTables will unlock all tables (idempotent)
func (agent *ActionAgent) UnlockTables(ctx context.Context) error {
	agent.mutex.Lock()
	defer agent.mutex.Unlock()

	if agent._lockTablesConnection == nil {
		// tables are already unlocked, so there is nothing to do
		return nil
	}

	return agent.unlockTablesHoldingMutex()
}

func (agent *ActionAgent) unlockTablesHoldingMutex() error {
	// We are cleaning up manually, let's kill the timer
	agent._lockTablesTimer.Stop()
	_, err := agent._lockTablesConnection.ExecuteFetch("UNLOCK TABLES", 0, false)
	if err != nil {
		return err
	}

	agent._lockTablesConnection.Close()
	agent._lockTablesConnection = nil
	agent._lockTablesTimer = nil

	return nil
}
