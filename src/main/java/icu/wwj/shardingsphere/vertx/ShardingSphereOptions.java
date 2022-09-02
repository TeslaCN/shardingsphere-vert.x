package icu.wwj.shardingsphere.vertx;

import io.vertx.sqlclient.SqlConnectOptions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class ShardingSphereOptions extends SqlConnectOptions {
    
    private final byte[] yamlConfigurationBytes;
}
