package io.vitess.jdbc;

import io.vitess.client.VTGateConn;
import java.sql.SQLException;

public interface VTGateConnectionProvider {
  void init(VitessJDBCUrl vitessJDBCUrl) throws SQLException;
  VTGateConn connect();
}
