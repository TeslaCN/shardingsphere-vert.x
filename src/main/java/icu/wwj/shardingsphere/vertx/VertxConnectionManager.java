package icu.wwj.shardingsphere.vertx;

import com.google.common.base.Strings;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.vertx.ExecutorVertxConnectionManager;
import org.apache.shardingsphere.mode.manager.ContextManager;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class VertxConnectionManager implements ExecutorVertxConnectionManager {
    
    private final Map<String, Pool> poolMap = new ConcurrentHashMap<>();
    
    private final Map<String, List<SqlConnection>> cachedConnections = new ConcurrentHashMap<>();
    
    private final Vertx vertx;
    
    private final ContextManager contextManager;
    
    private final ShardingSphereTransaction transaction;
    
    @Override
    public List<Future<? extends SqlClient>> getConnections(final String dataSourceName, final int connectionSize, final ConnectionMode connectionMode) {
        List<Future<? extends SqlClient>> result = new ArrayList<>(connectionSize);
        // TODO JDK-8161372 may occur here. But those use Vert.x may use higher version Java instead of 8.
        int connectionsToBeCreated = connectionSize;
        List<SqlConnection> cached = cachedConnections.computeIfAbsent(dataSourceName, unused -> new ArrayList<>());
        if (cached.size() >= connectionsToBeCreated) {
            for (int i = 0; i < connectionsToBeCreated; i++) {
                result.add(Future.succeededFuture(cached.get(i)));
            }
            return result;
        }
        for (SqlConnection each : cached) {
            result.add(Future.succeededFuture(each));
        }
        connectionsToBeCreated -= result.size();
        Pool pool = poolMap.computeIfAbsent(dataSourceName, this::createPool);
        for (int i = 0; i < connectionsToBeCreated; i++) {
            Future<SqlConnection> connection = pool.getConnection().onSuccess(sqlConnection -> cachedConnections.computeIfAbsent(dataSourceName, unused -> new ArrayList<>()).add(sqlConnection));
            if (TransactionStatus.IN_TRANSACTION == transaction.getTransactionStatus()) {
                connection.compose(SqlConnection::begin).onSuccess(transaction::addTransaction);
            }
            result.add(connection);
        }
        return result;
    }
    
    @SuppressWarnings("rawtypes")
    public Future<Void> clearCachedConnections() {
        List<Future> result = new ArrayList<>();
        for (List<SqlConnection> each : cachedConnections.values()) {
            for (SqlConnection eachConnection : each) {
                result.add(eachConnection.close());
            }
            each.clear();
        }
        return CompositeFuture.all(result).mapEmpty();
    }
    
    private Pool createPool(final String dataSourceName) {
        // TODO Support other DataSource
        HikariDataSource dataSource = (HikariDataSource) contextManager.getDataSourceMap("logic_db").get(dataSourceName);
        URI uri = URI.create(dataSource.getJdbcUrl().replace("jdbc:", ""));
        if (!"postgresql".equals(uri.getScheme())) {
            throw new UnsupportedOperationException("Support PostgreSQL only for now");
        }
        PgConnectOptions connectOptions = new PgConnectOptions().setHost(uri.getHost()).setPort(uri.getPort()).setDatabase(uri.getPath().replace("/", ""))
                .setUser(dataSource.getUsername()).setCachePreparedStatements(true).setPreparedStatementCacheMaxSize(16384);
        if (!Strings.isNullOrEmpty(dataSource.getPassword())) {
            connectOptions = connectOptions.setPassword(dataSource.getPassword());
        }
        PoolOptions poolOptions = new PoolOptions().setMaxSize(dataSource.getMaximumPoolSize()).setIdleTimeout((int) dataSource.getIdleTimeout()).setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
                .setConnectionTimeout((int) dataSource.getConnectionTimeout()).setConnectionTimeoutUnit(TimeUnit.MILLISECONDS);
        return PgPool.pool(vertx, connectOptions, poolOptions);
    }
}
