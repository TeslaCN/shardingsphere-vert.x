package icu.wwj.shardingsphere.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Tuple;
import org.apache.shardingsphere.infra.binder.QueryContext;
import org.apache.shardingsphere.infra.binder.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.context.ConnectionContext;
import org.apache.shardingsphere.infra.context.kernel.KernelProcessor;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeEngine;
import org.apache.shardingsphere.infra.executor.check.SQLCheckEngine;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupContext;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionContext;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.vertx.VertxExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.vertx.VertxExecutionContext;
import org.apache.shardingsphere.infra.parser.ShardingSphereSQLParserEngine;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    
    @Override
    public Future<RowSet<Row>> execute(final Tuple tuple) {
        ExecutionContext executionContext = createExecutionContext();
        DriverExecutionPrepareEngine<VertxExecutionUnit, Future<? extends SqlClient>> prepareEngine = new DriverExecutionPrepareEngine<>(
                "Vert.x", 1, backendConnection, new , new VertxExecutionContext(), rules, "PostgreSQL");
        ExecutionGroupContext<VertxExecutionUnit> executionGroupContext;
        try {
            executionGroupContext = prepareEngine.prepare(executionContext.getRouteContext(), executionContext.getExecutionUnits());
        } catch (final SQLException ex) {
            return Future.succeededFuture(getSaneExecuteResults(executionContext, ex));
        }
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
