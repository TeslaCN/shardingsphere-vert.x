package icu.wwj.shardingsphere.vertx;

import io.vertx.core.Future;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.vertx.ExecutorVertxStatementManager;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.vertx.VertxExecutionContext;

import java.sql.SQLException;

public class UnsupportedExecutorVertxStatementManager implements ExecutorVertxStatementManager {
    
    public static final ExecutorVertxStatementManager INSTANCE = new UnsupportedExecutorVertxStatementManager();
    
    @Override
    public Future<Query<RowSet<Row>>> createStorageResource(final Future<? extends SqlClient> connection, final ConnectionMode connectionMode, final VertxExecutionContext option) throws SQLException {
        return Future.failedFuture(new UnsupportedOperationException());
    }
    
    @Override
    public Future<Query<RowSet<Row>>> createStorageResource(final ExecutionUnit executionUnit, final Future<? extends SqlClient> connection, final ConnectionMode connectionMode, final VertxExecutionContext option) throws SQLException {
        return Future.failedFuture(new UnsupportedOperationException());
    }
}
