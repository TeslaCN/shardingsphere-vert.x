package icu.wwj.shardingsphere.vertx;

import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlClient;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.vertx.ExecutorVertxConnectionManager;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VertxConnectionManager implements ExecutorVertxConnectionManager {
    
    private final Map<String, Pool> poolMap = new HashMap<>();
    
    @Override
    public List<Future<? extends SqlClient>> getConnections(final String dataSourceName, final int connectionSize, final ConnectionMode connectionMode) throws SQLException {
        return null;
    }
}
