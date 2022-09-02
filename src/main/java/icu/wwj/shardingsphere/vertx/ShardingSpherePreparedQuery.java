package icu.wwj.shardingsphere.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Tuple;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.context.kernel.KernelProcessor;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;

@RequiredArgsConstructor
public class ShardingSpherePreparedQuery implements PreparedQuery<RowSet<Row>> {
    
    private final ShardingSphereConnection connection;
    
    private final MetaDataContexts metaDataContexts;
    
    private final KernelProcessor kernelProcessor = new KernelProcessor();
    
    private final String sql;
    
    private SQLStatementContext<?> sqlStatementContext;
    
    @Override
    public void execute(final Tuple tuple, final Handler<AsyncResult<RowSet<Row>>> handler) {
        execute(tuple).onComplete(handler);
    }
    
    @Override
    public Future<RowSet<Row>> execute(final Tuple tuple) {
        return null;
    }
    
    @Override
    public void executeBatch(final List<Tuple> batch, final Handler<AsyncResult<RowSet<Row>>> handler) {
        executeBatch(batch).onComplete(handler);
    }
    
    @Override
    public Future<RowSet<Row>> executeBatch(final List<Tuple> batch) {
        return null;
    }
    
    @Override
    public void execute(final Handler<AsyncResult<RowSet<Row>>> handler) {
        execute().onComplete(handler);
    }
    
    @Override
    public Future<RowSet<Row>> execute() {
        return null;
    }
    
    @Override
    public <R> PreparedQuery<SqlResult<R>> collecting(final Collector<Row, ?, R> collector) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public <U> PreparedQuery<RowSet<U>> mapping(final Function<Row, U> mapper) {
        throw new UnsupportedOperationException();
    }
}
