package icu.wwj.shardingsphere.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.PrepareOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.spi.DatabaseMetadata;

public class ShardingSphereConnection implements SqlConnection {
    
    @Override
    public SqlConnection prepare(final String sql, final Handler<AsyncResult<PreparedStatement>> handler) {
        return null;
    }
    
    @Override
    public Future<PreparedStatement> prepare(final String sql) {
        return null;
    }
    
    @Override
    public SqlConnection prepare(final String sql, final PrepareOptions options, final Handler<AsyncResult<PreparedStatement>> handler) {
        return null;
    }
    
    @Override
    public Future<PreparedStatement> prepare(final String sql, final PrepareOptions options) {
        return null;
    }
    
    @Override
    public SqlConnection exceptionHandler(final Handler<Throwable> handler) {
        return null;
    }
    
    @Override
    public SqlConnection closeHandler(final Handler<Void> handler) {
        return null;
    }
    
    @Override
    public void begin(final Handler<AsyncResult<Transaction>> handler) {
        
    }
    
    @Override
    public Future<Transaction> begin() {
        return null;
    }
    
    @Override
    public boolean isSSL() {
        return false;
    }
    
    @Override
    public Query<RowSet<Row>> query(final String sql) {
        return null;
    }
    
    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(final String sql) {
        return null;
    }
    
    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(final String sql, final PrepareOptions options) {
        return null;
    }
    
    @Override
    public void close(final Handler<AsyncResult<Void>> handler) {
        close().onComplete(handler);
    }
    
    @Override
    public Future<Void> close() {
        return null;
    }
    
    @Override
    public DatabaseMetadata databaseMetadata() {
        return null;
    }
}
