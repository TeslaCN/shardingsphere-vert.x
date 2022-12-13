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
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.binder.QueryContext;
import org.apache.shardingsphere.infra.binder.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.binder.aware.ParameterAware;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.context.kernel.KernelProcessor;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeEngine;
import org.apache.shardingsphere.infra.executor.check.SQLCheckEngine;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupContext;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutorCallback;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionContext;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.vertx.VertxExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.result.ExecuteResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.impl.driver.vertx.VertxQueryResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.update.UpdateResult;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.vertx.VertxExecutionContext;
import org.apache.shardingsphere.infra.merge.MergeEngine;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.parser.ShardingSphereSQLParserEngine;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;

public class ShardingSpherePreparedQuery implements PreparedQuery<RowSet<Row>> {
    
    private final KernelProcessor kernelProcessor = new KernelProcessor();
    
    private final ShardingSphereConnection connection;
    
    private final MetaDataContexts metaDataContexts;
    
    private final String sql;
    
    private final SQLStatementContext<?> sqlStatementContext;
    
    private final QueryContext queryContext;
    
    private Map<String, Integer> columnLabelAndIndexMap;
    
    public ShardingSpherePreparedQuery(final ShardingSphereConnection connection, final MetaDataContexts metaDataContexts, final String sql) {
        this.connection = connection;
        this.metaDataContexts = metaDataContexts;
        this.sql = sql;
        SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
        ShardingSphereSQLParserEngine sqlParserEngine = sqlParserRule.getSQLParserEngine(
                DatabaseTypeEngine.getTrunkDatabaseTypeName(metaDataContexts.getMetaData().getDatabase("logic_db").getProtocolType()));
        SQLStatement sqlStatement = sqlParserEngine.parse(sql, true);
        sqlStatementContext = SQLStatementContextFactory.newInstance(metaDataContexts.getMetaData(), sqlStatement, "logic_db");
        queryContext = new QueryContext(sqlStatementContext, sql, new ArrayList<>());
    }
    
    @Override
    public void execute(final Tuple tuple, final Handler<AsyncResult<RowSet<Row>>> handler) {
        execute(tuple).onComplete(handler);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @SneakyThrows(SQLException.class)
    @Override
    public Future<RowSet<Row>> execute(final Tuple tuple) {
        setupParameters(tuple);
        ExecutionContext executionContext = createExecutionContext();
        ShardingSphereDatabase database = metaDataContexts.getMetaData().getDatabase("logic_db");
        Collection<ShardingSphereRule> rules = database.getRuleMetaData().getRules();
        Map<String, DatabaseType> storageTypes = database.getResourceMetaData().getStorageTypes();
        DriverExecutionPrepareEngine<VertxExecutionUnit, Future<? extends SqlClient>> prepareEngine = new DriverExecutionPrepareEngine<>(
                "Vert.x", 1, connection.getConnectionManager(), UnsupportedExecutorVertxStatementManager.INSTANCE, new VertxExecutionContext(), rules, storageTypes);
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
                        : new VertxQueryResult(new VertxQueryResultMetaData(rowSet.columnDescriptors(), rowSet.size()), rowSet.iterator());
            }
        });
        return CompositeFuture.all((List) executeResults).map(future -> {
            List results = future.list();
            if (results.get(0) instanceof UpdateResult) {
                return fromUpdateResult(results);
            }
            MergeEngine mergeEngine = new MergeEngine(database,
                    metaDataContexts.getMetaData().getProps(), connection.getConnectionContext());
            try {
                return fromMergeResult(results, mergeEngine.merge(results, executionContext.getSqlStatementContext()));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    private RowSet<Row> fromUpdateResult(final List<UpdateResult> updateResults) {
        int updated = 0;
        for (UpdateResult each : updateResults) {
            updated += each.getUpdateCount();
        }
        return new ShardingSphereUpdateRowSet(updated);
    }
    
    private RowSet<Row> fromMergeResult(final List<QueryResult> queryResults, final MergedResult mergedResult) {
        Map<String, Integer> columnLabelAndIndexMap = null != this.columnLabelAndIndexMap ? this.columnLabelAndIndexMap
                : (this.columnLabelAndIndexMap = RowSetUtil.createColumnLabelAndIndexMap(queryResults.get(0).getMetaData()));
        int size = 0;
        for (QueryResult each : queryResults) {
            size += ((VertxQueryResultMetaData)each.getMetaData()).getRowCount();
        }
        return new ShardingSphereRowSet(queryResults.get(0),columnLabelAndIndexMap, mergedResult, size);
    }
    
    private void setupParameters(final Tuple tuple) {
        List<Object> parameters = new ArrayList<>(tuple.size());
        for (int i = 0; i < tuple.size(); i++) {
            parameters.add(tuple.getValue(i));
        }
        queryContext.getParameters().clear();
        queryContext.getParameters().addAll(parameters);
        if (sqlStatementContext instanceof ParameterAware) {
            ((ParameterAware) sqlStatementContext).setUpParameters(parameters);
        }
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
    
    @SneakyThrows(SQLException.class)
    @Override
    public Future<RowSet<Row>> executeBatch(final List<Tuple> batch) {
        if (null == batch || batch.isEmpty()) {
            return Future.failedFuture(new IllegalArgumentException("Empty batch"));
        }
        Map<ExecutionUnit, List<Tuple>> executionUnitParameters = new HashMap<>();
        Iterator<Tuple> tupleIterator = batch.iterator();
        Tuple tuple = tupleIterator.next();
        List<Object> firstGroupOfParameter = fromTuple(tuple);
        if (sqlStatementContext instanceof ParameterAware) {
            ((ParameterAware) sqlStatementContext).setUpParameters(firstGroupOfParameter);
        }
        ExecutionContext executionContext = createExecutionContext(new QueryContext(sqlStatementContext, sql, firstGroupOfParameter));
        for (ExecutionUnit each : executionContext.getExecutionUnits()) {
            executionUnitParameters.computeIfAbsent(each, unused -> new LinkedList<>()).add(tuple);
        }
        while (tupleIterator.hasNext()) {
            tuple = tupleIterator.next();
            List<Object> eachGroupOfParameter = fromTuple(tuple);
            if (sqlStatementContext instanceof ParameterAware) {
                ((ParameterAware) sqlStatementContext).setUpParameters(eachGroupOfParameter);
            }
            ExecutionContext eachExecutionContext = createExecutionContext(new QueryContext(sqlStatementContext, sql, eachGroupOfParameter));
            for (ExecutionUnit each : eachExecutionContext.getExecutionUnits()) {
                executionUnitParameters.computeIfAbsent(each, unused -> new LinkedList<>()).add(tuple);
            }
        }
        ExecutionGroupContext<VertxExecutionUnit> executionGroupContext = addBatchedParametersToPreparedStatements(executionContext, executionUnitParameters.keySet());
        return executeBatchedPreparedStatements(executionGroupContext, executionUnitParameters);
    }
    
    private List<Object> fromTuple(final Tuple tuple) {
        List<Object> result = new ArrayList<>(tuple.size());
        for (int i = 0; i < tuple.size(); i++) {
            result.add(tuple.getValue(i));
        }
        return result;
    }
    
    private ExecutionContext createExecutionContext(final QueryContext queryContext) {
        String databaseName = "logic_db";
        SQLCheckEngine.check(queryContext.getSqlStatementContext(), queryContext.getParameters(),
                metaDataContexts.getMetaData().getDatabase(databaseName).getRuleMetaData().getRules(),
                databaseName, metaDataContexts.getMetaData().getDatabases(), null);
        return kernelProcessor.generateExecutionContext(queryContext, metaDataContexts.getMetaData().getDatabase(databaseName),
                metaDataContexts.getMetaData().getGlobalRuleMetaData(), metaDataContexts.getMetaData().getProps(), connection.getConnectionContext());
    }
    
    private ExecutionGroupContext<VertxExecutionUnit> addBatchedParametersToPreparedStatements(final ExecutionContext executionContext, final Set<ExecutionUnit> executionUnits) throws SQLException {
        ShardingSphereDatabase database = metaDataContexts.getMetaData().getDatabase("logic_db");
        Collection<ShardingSphereRule> rules = database.getRuleMetaData().getRules();
        DriverExecutionPrepareEngine<VertxExecutionUnit, Future<? extends SqlClient>> prepareEngine = new DriverExecutionPrepareEngine<>(
                "Vert.x", metaDataContexts.getMetaData().getProps().<Integer>getValue(ConfigurationPropertyKey.MAX_CONNECTIONS_SIZE_PER_QUERY),
                connection.getConnectionManager(), UnsupportedExecutorVertxStatementManager.INSTANCE, new VertxExecutionContext(), rules, database.getResourceMetaData().getStorageTypes());
        return prepareEngine.prepare(executionContext.getRouteContext(), executionUnits);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Future<RowSet<Row>> executeBatchedPreparedStatements(final ExecutionGroupContext<VertxExecutionUnit> executionGroupContext, final Map<ExecutionUnit, List<Tuple>> executionUnitParameters) {
        ExecutorCallback<VertxExecutionUnit, Future<ExecuteResult>> callback = new BatchedInsertsVertxExecutorCallback(executionUnitParameters);
        List futures = ShardingSphereVertxExecutor.execute(executionGroupContext, callback);
        return CompositeFuture.all(futures).map(rowSets -> {
            int result = 0;
            for (UpdateResult each : rowSets.<UpdateResult>list()) {
                result += each.getUpdateCount();
            }
            return new ShardingSphereUpdateRowSet(result);
        });
    }
    
    @RequiredArgsConstructor
    private static class BatchedInsertsVertxExecutorCallback implements ExecutorCallback<VertxExecutionUnit, Future<ExecuteResult>> {
        
        private final Map<ExecutionUnit, List<Tuple>> executionUnitParameters;
        
        @Override
        public Collection<Future<ExecuteResult>> execute(final Collection<VertxExecutionUnit> inputs, final boolean isTrunkThread, final Map<String, Object> dataMap) throws SQLException {
            List<Future<ExecuteResult>> result = new ArrayList<>(inputs.size());
            for (VertxExecutionUnit unit : inputs) {
                List<Tuple> tuples = executionUnitParameters.get(unit.getExecutionUnit());
                result.add(unit.getStorageResource().compose(pq -> pq.executeBatch(tuples)).map(r -> new UpdateResult(r.rowCount(), 0)));
            }
            return result;
        }
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
