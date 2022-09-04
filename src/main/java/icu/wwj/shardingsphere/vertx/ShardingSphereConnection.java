package icu.wwj.shardingsphere.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.PrepareOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.spi.DatabaseMetadata;
import lombok.Getter;
import org.apache.shardingsphere.infra.context.ConnectionContext;
import org.apache.shardingsphere.mode.manager.ContextManager;

public class ShardingSphereConnection implements SqlConnection {
    
    private final Vertx vertx;
    
    private final ContextManager contextManager;
    
    @Getter
    private final VertxConnectionManager connectionManager;
    
    @Getter
    private final ConnectionContext connectionContext = new ConnectionContext();
    
    private final ShardingSphereTransaction transaction = new ShardingSphereTransaction(this);
    
    public ShardingSphereConnection(final Vertx vertx, final ContextManager contextManager) {
        this.vertx = vertx;
        this.contextManager = contextManager;
        connectionManager = new VertxConnectionManager(vertx, contextManager, transaction);
    }
    
    @Override
    public SqlConnection prepare(final String sql, final Handler<AsyncResult<PreparedStatement>> handler) {
        prepare(sql).onComplete(handler);
        return this;
    }
    
    @Override
    public Future<PreparedStatement> prepare(final String sql) {
        return Future.failedFuture(new UnsupportedOperationException());
    }
    
    @Override
    public SqlConnection prepare(final String sql, final PrepareOptions options, final Handler<AsyncResult<PreparedStatement>> handler) {
        prepare(sql, options).onComplete(handler);
        return this;
    }
    
    @Override
    public Future<PreparedStatement> prepare(final String sql, final PrepareOptions options) {
        return Future.failedFuture(new UnsupportedOperationException());
    }
    
    @Override
    public SqlConnection exceptionHandler(final Handler<Throwable> handler) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public SqlConnection closeHandler(final Handler<Void> handler) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void begin(final Handler<AsyncResult<Transaction>> handler) {
        begin().onComplete(handler);
    }
    
    @Override
    public Future<Transaction> begin() {
        return transaction.begin();
    }
    
    @Override
    public boolean isSSL() {
        return false;
    }
    
    @Override
    public Query<RowSet<Row>> query(final String sql) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(final String sql) {
        return new ShardingSpherePreparedQuery(this, contextManager.getMetaDataContexts(), sql);
    }
    
    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(final String sql, final PrepareOptions options) {
        return new ShardingSpherePreparedQuery(this, contextManager.getMetaDataContexts(), sql);
    }
    
    @Override
    public void close(final Handler<AsyncResult<Void>> handler) {
        close().onComplete(handler);
    }
    
    @Override
    public Future<Void> close() {
        return Future.succeededFuture();
    }
    
    @Override
    public DatabaseMetadata databaseMetadata() {
        throw new UnsupportedOperationException();
    }
}
