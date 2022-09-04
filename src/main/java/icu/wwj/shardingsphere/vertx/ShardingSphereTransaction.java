package icu.wwj.shardingsphere.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.sqlclient.Transaction;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("rawtypes")
@RequiredArgsConstructor
public class ShardingSphereTransaction implements Transaction {
    
    private final ShardingSphereConnection connection;
    
    @Getter
    private final List<Transaction> openedTransactions = new ArrayList<>();
    
    private final AtomicReference<TransactionStatus> status = new AtomicReference<>(TransactionStatus.NOT_IN_TRANSACTION);
    
    public TransactionStatus getTransactionStatus() {
        return status.get();
    }
    
    public Future<Transaction> begin() {
        return status.compareAndSet(TransactionStatus.NOT_IN_TRANSACTION, TransactionStatus.IN_TRANSACTION) ? Future.succeededFuture(this)
                : Future.failedFuture(new InvalidTransactionStatusException("Cannot begin transaction, current status is " + status.get().name()));
    }
    
    public void addTransaction(final Transaction transaction) {
        openedTransactions.add(transaction);
    }
    
    @Override
    public Future<Void> commit() {
        if (!status.compareAndSet(TransactionStatus.IN_TRANSACTION, TransactionStatus.NOT_IN_TRANSACTION)) {
            return Future.failedFuture(new InvalidTransactionStatusException("Cannot commit transaction, current status is" + status.get().name()));
        }
        List<Future> commitFutures = new ArrayList<>(openedTransactions.size());
        for (Transaction each : openedTransactions) {
            commitFutures.add(each.rollback());
        }
        return CompositeFuture.all(commitFutures).eventually(unused -> {
            openedTransactions.clear();
            return Future.succeededFuture();
        }).mapEmpty();
    }
    
    @Override
    public void commit(final Handler<AsyncResult<Void>> handler) {
        commit().onComplete(handler);
    }
    
    @Override
    public Future<Void> rollback() {
        if (TransactionStatus.NOT_IN_TRANSACTION == status.get()) {
            return Future.failedFuture(new InvalidTransactionStatusException("Cannot rollback transaction, current status is " + status.get().name()));
        }
        List<Future> rollbackFutures = new ArrayList<>(openedTransactions.size());
        for (Transaction each : openedTransactions) {
            rollbackFutures.add(each.rollback());
        }
        return CompositeFuture.all(rollbackFutures).eventually(unused -> {
            openedTransactions.clear();
            return Future.succeededFuture();
        }).mapEmpty();
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
        throw new UnsupportedOperationException();
    }
}
