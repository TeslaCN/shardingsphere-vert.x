package icu.wwj.shardingsphere.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.Transaction;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ShardingSphereTransaction implements Transaction {
    
    private final ShardingSphereConnection connection;
    
    @Override
    public Future<Void> commit() {
        return null;
    }
    
    @Override
    public void commit(final Handler<AsyncResult<Void>> handler) {
        commit().onComplete(handler);
    }
    
    @Override
    public Future<Void> rollback() {
        return null;
    }
    
    @Override
    public void rollback(final Handler<AsyncResult<Void>> handler) {
        rollback().onComplete(handler);
    }
    
    @Override
    public void completion(final Handler<AsyncResult<Void>> handler) {
        completion().onComplete(handler);
    }
    
    @Override
    public Future<Void> completion() {
        return null;
    }
}
