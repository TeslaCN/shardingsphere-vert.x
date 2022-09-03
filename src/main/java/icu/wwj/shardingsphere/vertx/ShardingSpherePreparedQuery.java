package icu.wwj.shardingsphere.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Tuple;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.binder.QueryContext;
import org.apache.shardingsphere.infra.binder.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.context.kernel.KernelProcessor;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeEngine;
import org.apache.shardingsphere.infra.executor.check.SQLCheckEngine;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupContext;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutorCallback;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionContext;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.vertx.VertxExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.result.ExecuteResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.impl.driver.vertx.VertxQueryResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.update.UpdateResult;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.vertx.VertxExecutionContext;
import org.apache.shardingsphere.infra.merge.MergeEngine;
import org.apache.shardingsphere.infra.parser.ShardingSphereSQLParserEngine;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

public class ShardingSpherePreparedQuery implements PreparedQuery<RowSet<Row>> {
    
    private final KernelProcessor kernelProcessor = new KernelProcessor();
    
    private final ShardingSphereConnection connection;
    
    private final MetaDataContexts metaDataContexts;
    
    private final String sql;
    
    private final SQLStatement sqlStatement;
    
    private final SQLStatementContext<?> sqlStatementContext;
    
    private final QueryContext queryContext;
    
    public ShardingSpherePreparedQuery(final ShardingSphereConnection connection, final MetaDataContexts metaDataContexts, final String sql) {
        this.connection = connection;
        this.metaDataContexts = metaDataContexts;
        this.sql = sql;
        SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
        ShardingSphereSQLParserEngine sqlParserEngine = sqlParserRule.getSQLParserEngine(
                DatabaseTypeEngine.getTrunkDatabaseTypeName(metaDataContexts.getMetaData().getDatabase("logic_db").getResource().getDatabaseType()));
        sqlStatement = sqlParserEngine.parse(sql, true);
        sqlStatementContext = SQLStatementContextFactory.newInstance(metaDataContexts.getMetaData().getDatabases(), sqlStatement, "logic_db");
        queryContext = new QueryContext(sqlStatementContext, sql, new ArrayList<>());
    }
    
    @Override
    public void execute(final Tuple tuple, final Handler<AsyncResult<RowSet<Row>>> handler) {
        execute(tuple).onComplete(handler);
    }
    
    @SneakyThrows(SQLException.class)
    @Override
    public Future<RowSet<Row>> execute(final Tuple tuple) {
        ExecutionContext executionContext = createExecutionContext();
        DriverExecutionPrepareEngine<VertxExecutionUnit, Future<? extends SqlClient>> prepareEngine = new DriverExecutionPrepareEngine<>(
                "Vert.x", 1, connection.getConnectionManager(), UnsupportedExecutorVertxStatementManager.INSTANCE, new VertxExecutionContext(), rules, "PostgreSQL");
        ExecutionGroupContext<VertxExecutionUnit> executionGroupContext = prepareEngine.prepare(executionContext.getRouteContext(), executionContext.getExecutionUnits());
        List<Future<ExecuteResult>> executeResults = ShardingSphereVertxExecutor.execute(executionGroupContext, new ExecutorCallback<VertxExecutionUnit, Future<ExecuteResult>>() {
            
            @Override
            public Collection<Future<ExecuteResult>> execute(final Collection<VertxExecutionUnit> inputs, final boolean isTrunkThread, final Map<String, Object> dataMap) {
                List<Future<ExecuteResult>> result = new ArrayList<>(inputs.size());
                for (VertxExecutionUnit each : inputs) {
                    Future<RowSet<Row>> future = each.getStorageResource().compose(preparedQuery -> preparedQuery.execute(Tuple.from(each.getExecutionUnit().getSqlUnit().getParameters())));
                    result.add(future.map(this::handleResult));
                }
                return result;
            }
            
            private ExecuteResult handleResult(final RowSet<Row> rowSet) {
                return null == rowSet.columnDescriptors() ? new UpdateResult(rowSet.rowCount(), 0L)
                        : new VertxQueryResult(new VertxQueryResultMetaData(rowSet.columnDescriptors()), rowSet.iterator());
            }
        });
        CompositeFuture.all((List) executeResults).map(future -> {
            if (future.list().get(0) instanceof UpdateResult) {
                
            }
            MergeEngine mergeEngine = new MergeEngine(metaDataContexts.getMetaData().getDatabase("logic_db"),
                    metaDataContexts.getMetaData().getProps(), connection.getConnectionContext());
            try {
                return mergeEngine.merge(future.list(), executionContext.getSqlStatementContext());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        return null;
    }
    
    private ExecutionContext createExecutionContext() {
        String databaseName = "logic_db";
        SQLCheckEngine.check(queryContext.getSqlStatementContext(), queryContext.getParameters(),
                metaDataContexts.getMetaData().getDatabase(databaseName).getRuleMetaData().getRules(),
                databaseName, metaDataContexts.getMetaData().getDatabases(), null);
        return kernelProcessor.generateExecutionContext(queryContext, metaDataContexts.getMetaData().getDatabase(databaseName),
                metaDataContexts.getMetaData().getGlobalRuleMetaData(), metaDataContexts.getMetaData().getProps(), connection.getConnectionContext());
    }
    
    @Override
    public void executeBatch(final List<Tuple> batch, final Handler<AsyncResult<RowSet<Row>>> handler) {
        executeBatch(batch).onComplete(handler);
    }
    
    @Override
    public Future<RowSet<Row>> executeBatch(final List<Tuple> batch) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void execute(final Handler<AsyncResult<RowSet<Row>>> handler) {
        execute().onComplete(handler);
    }
    
    @Override
    public Future<RowSet<Row>> execute() {
        throw new UnsupportedOperationException();
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
