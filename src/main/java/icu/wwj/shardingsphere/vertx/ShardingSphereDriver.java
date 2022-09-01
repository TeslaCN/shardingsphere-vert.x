package icu.wwj.shardingsphere.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.impl.CloseFuture;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.spi.ConnectionFactory;
import io.vertx.sqlclient.spi.Driver;

import java.util.List;

public class ShardingSphereDriver implements Driver {
    
    @Override
    public Pool newPool(final Vertx vertx, final List<? extends SqlConnectOptions> databases, final PoolOptions options, final CloseFuture closeFuture) {
        return null;
    }
    
    @Override
    public ConnectionFactory createConnectionFactory(final Vertx vertx, final SqlConnectOptions database) {
        return null;
    }
    
    @Override
    public SqlConnectOptions parseConnectionUri(final String uri) {
        return null;
    }
    
    @Override
    public boolean acceptsOptions(final SqlConnectOptions connectOptions) {
        return false;
    }
}
