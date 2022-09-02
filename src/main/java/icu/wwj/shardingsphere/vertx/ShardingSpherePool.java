package icu.wwj.shardingsphere.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PrepareOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.mode.manager.ContextManager;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@RequiredArgsConstructor
public class ShardingSpherePool implements Pool {
    
    private final Vertx vertx;
    
    private final ContextManager contextManager;
    
    private final AtomicInteger size = new AtomicInteger();
    
    @Override
    public void getConnection(final Handler<AsyncResult<SqlConnection>> handler) {
        getConnection().onComplete(handler);
    }
    
    @Override
    public Future<SqlConnection> getConnection() {
        return Future.succeededFuture(new ShardingSphereConnection());
    }
    
    @Override
    public Query<RowSet<Row>> query(final String sql) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(final String sql) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(final String sql, final PrepareOptions options) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void close(final Handler<AsyncResult<Void>> handler) {
        
    }
    
    @Override
    public Future<Void> close() {
        return null;
    }
    
    @Override
    public Pool connectHandler(final Handler<SqlConnection> handler) {
        return null;
    }
    
    @Override
    public Pool connectionProvider(final Function<Context, Future<SqlConnection>> provider) {
        return null;
    }
    
    @Override
    public int size() {
        return size.get();
    }
}
