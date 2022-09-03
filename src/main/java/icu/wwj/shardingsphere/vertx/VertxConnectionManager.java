package icu.wwj.shardingsphere.vertx;

import com.google.common.base.Strings;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.vertx.ExecutorVertxConnectionManager;
import org.apache.shardingsphere.mode.manager.ContextManager;

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class VertxConnectionManager implements ExecutorVertxConnectionManager {
    
    private final Map<String, Pool> poolMap = new ConcurrentHashMap<>();
    
    private final Vertx vertx;
    
    private final ContextManager contextManager;
    
    @Override
    public List<Future<? extends SqlClient>> getConnections(final String dataSourceName, final int connectionSize, final ConnectionMode connectionMode) throws SQLException {
        List<Future<? extends SqlClient>> result = new ArrayList<>(connectionSize);
        // Those use Vert.x may use higher version Java instead of 8
        Pool pool = poolMap.computeIfAbsent(dataSourceName, this::createPool);
        for (int i = 0; i < connectionSize; i++) {
            result.add(pool.getConnection());
        }
        return result;
    }
    
    private Pool createPool(final String dataSourceName) {
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
