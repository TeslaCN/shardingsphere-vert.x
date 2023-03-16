# ShardingSphere on Vert.x API

## Getting Started

### Step 1: Create a ShardingSphere-JDBC YAML configuration

```yaml
dataSources:
  ds_0:
    dataSourceClassName: com.zaxxer.hikari.HikariDataSource
    driverClassName: org.postgresql.Driver
    jdbcUrl: jdbc:postgresql://127.0.0.1:5432/bmsql_0
    maximumPoolSize: 65
    minimumIdle: 0
    password: postgres
    username: postgres
    connectionTimeout: 30000
    # Configure your DataSource just like the way using ShardingSphere-JDBC
  ds_3:
    dataSourceClassName: com.zaxxer.hikari.HikariDataSource
    driverClassName: org.postgresql.Driver
    jdbcUrl: jdbc:postgresql://127.0.0.1:5432/bmsql_3
    maximumPoolSize: 65
    minimumIdle: 0
    password: postgres
    username: postgres
    connectionTimeout: 30000
props:
#  max-connections-size-per-query: 1
#  sql-show: true
rules:
  - !SHARDING
# Omitted details of Sharding rule
```

### Step 2: Create Vert.x Pool & Enjoy ShardingSphere-Vert.x

```java
String uri = "shardingsphere:/path/to/your/config-sharding.yaml";
Vertx vertx = Vertx.vertx(new VertxOptions(/* Your Vert.x options */));
// cachePreparedStatements could not be specified in URI. https://github.com/eclipse-vertx/vertx-sql-client/issues/664
SqlConnectOptions connectOptions = SqlConnectOptions.fromUri(uri).setCachePreparedStatements(true);
Pool pool = Pool.pool(vertx, connectOptions, new PoolOptions(/* Your Pool options */));
// Just use the `pool` to do what you like.
```

## How to Build

```bash
git clone https://github.com/TeslaCN/tpcc-vertx.git
cd tpcc-vertx
./mvnw clean install
```
