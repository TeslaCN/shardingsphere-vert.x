package icu.wwj.shardingsphere.vertx;

import io.vertx.core.Future;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.executor.kernel.ExecutorEngine;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupContext;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutorCallback;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.vertx.VertxExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.result.ExecuteResult;

import java.sql.SQLException;
import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ShardingSphereVertxExecutor {
    
    private static final ExecutorEngine EXECUTOR_ENGINE = ExecutorEngine.createExecutorEngineWithSize(0);
    
    @SneakyThrows(SQLException.class)
    public static List<Future<ExecuteResult>> execute(final ExecutionGroupContext<VertxExecutionUnit> executionGroupContext, final ExecutorCallback<VertxExecutionUnit, Future<ExecuteResult>> callback) {
        return EXECUTOR_ENGINE.execute(executionGroupContext, null, callback, true);
    }
}
