package icu.wwj.shardingsphere.vertx;

import com.google.common.base.Strings;
import io.vertx.core.Vertx;
import io.vertx.core.impl.CloseFuture;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.spi.ConnectionFactory;
import io.vertx.sqlclient.spi.Driver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.config.database.impl.DataSourceProvidedDatabaseConfiguration;
import org.apache.shardingsphere.infra.config.rule.RuleConfiguration;
import org.apache.shardingsphere.infra.config.rule.scope.GlobalRuleConfiguration;
import org.apache.shardingsphere.infra.datasource.pool.destroyer.DataSourcePoolDestroyer;
import org.apache.shardingsphere.infra.instance.metadata.InstanceMetaData;
import org.apache.shardingsphere.infra.instance.metadata.InstanceMetaDataBuilderFactory;
import org.apache.shardingsphere.infra.util.yaml.YamlEngine;
import org.apache.shardingsphere.infra.yaml.config.pojo.YamlRootConfiguration;
import org.apache.shardingsphere.infra.yaml.config.swapper.resource.YamlDataSourceConfigurationSwapper;
import org.apache.shardingsphere.infra.yaml.config.swapper.rule.YamlRuleConfigurationSwapperEngine;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.mode.manager.ContextManagerBuilderFactory;
import org.apache.shardingsphere.mode.manager.ContextManagerBuilderParameter;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class ShardingSphereDriver implements Driver {
    
    static {
        log.info("███████ ██   ██  █████  ██████  ██████  ██ ███    ██  ██████  ███████ ██████  ██   ██ ███████ ██████  ███████ \n" +
                "██      ██   ██ ██   ██ ██   ██ ██   ██ ██ ████   ██ ██       ██      ██   ██ ██   ██ ██      ██   ██ ██      \n" +
                "███████ ███████ ███████ ██████  ██   ██ ██ ██ ██  ██ ██   ███ ███████ ██████  ███████ █████   ██████  █████   \n" +
                "     ██ ██   ██ ██   ██ ██   ██ ██   ██ ██ ██  ██ ██ ██    ██      ██ ██      ██   ██ ██      ██   ██ ██      \n" +
                "███████ ██   ██ ██   ██ ██   ██ ██████  ██ ██   ████  ██████  ███████ ██      ██   ██ ███████ ██   ██ ███████ \n" +
                "                                                                                                              \n" +
                "                                                                                                              \n" +
                "██    ██ ███████ ██████  ████████    ██   ██                                                                  \n" +
                "██    ██ ██      ██   ██    ██        ██ ██                                                                   \n" +
                "██    ██ █████   ██████     ██         ███                                                                    \n" +
                " ██  ██  ██      ██   ██    ██        ██ ██                                                                   \n" +
                "  ████   ███████ ██   ██    ██    ██ ██   ██                                                                  \n");
    }
    
    private static final YamlRuleConfigurationSwapperEngine SWAPPER_ENGINE = new YamlRuleConfigurationSwapperEngine();
    
    private static final YamlDataSourceConfigurationSwapper DATA_SOURCE_SWAPPER = new YamlDataSourceConfigurationSwapper();
    
    @SneakyThrows
    @Override
    public Pool newPool(final Vertx vertx, final List<? extends SqlConnectOptions> databases, final PoolOptions options, final CloseFuture closeFuture) {
        ShardingSphereOptions shardingSphereOption = (ShardingSphereOptions) databases.get(0);
        YamlRootConfiguration rootConfig = YamlEngine.unmarshal(shardingSphereOption.getYamlConfigurationBytes(), YamlRootConfiguration.class);
        Map<String, DataSource> dataSourceMap = DATA_SOURCE_SWAPPER.swapToDataSources(rootConfig.getDataSources());
        Collection<RuleConfiguration> ruleConfigs = SWAPPER_ENGINE.swapToRuleConfigurations(rootConfig.getRules());
        String databaseName = Strings.isNullOrEmpty(rootConfig.getDatabaseName()) ? "logic_db" : rootConfig.getDatabaseName();
        ContextManager contextManager = createContextManager(databaseName, dataSourceMap, ruleConfigs, rootConfig.getProps());
        for (DataSource each : dataSourceMap.values()) {
            new DataSourcePoolDestroyer(each).asyncDestroy();
        }
        return new ShardingSpherePool(vertx, contextManager);
    }
    
    private ContextManager createContextManager(final String databaseName, final Map<String, DataSource> dataSourceMap,
                                                final Collection<RuleConfiguration> ruleConfigs, final Properties props) throws SQLException {
        InstanceMetaData instanceMetaData = InstanceMetaDataBuilderFactory.create("JDBC", -1);
        Collection<RuleConfiguration> globalRuleConfigs = ruleConfigs.stream().filter(each -> each instanceof GlobalRuleConfiguration).collect(Collectors.toList());
        Collection<RuleConfiguration> databaseRuleConfigs = new LinkedList<>(ruleConfigs);
        databaseRuleConfigs.removeAll(globalRuleConfigs);
        ContextManagerBuilderParameter parameter = new ContextManagerBuilderParameter(null, Collections.singletonMap(databaseName,
                new DataSourceProvidedDatabaseConfiguration(dataSourceMap, databaseRuleConfigs)), globalRuleConfigs, props, Collections.emptyList(), instanceMetaData);
        // TODO Supports standalone mode only for now
        return ContextManagerBuilderFactory.getInstance(null).build(parameter);
    }
    
    @Override
    public ConnectionFactory createConnectionFactory(final Vertx vertx, final SqlConnectOptions database) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public SqlConnectOptions parseConnectionUri(final String uri) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean acceptsOptions(final SqlConnectOptions connectOptions) {
        return connectOptions instanceof ShardingSphereOptions;
    }
}
