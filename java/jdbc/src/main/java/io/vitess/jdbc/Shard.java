package io.vitess.jdbc;

public class Shard {
  public final String keyspace;
  public final String shard;

  public Shard(String keyspace, String shard) {
    this.keyspace = keyspace;
    this.shard = shard;
  }
}
