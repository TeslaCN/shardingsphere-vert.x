package icu.wwj.shardingsphere.vertx;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.merge.result.MergedResult;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class ShardingSphereRow implements Row {
    
    private final QueryResult queryResultSample;
    
    private final Map<String, Integer> columnLabelAndIndexMap;
    
    private final MergedResult mergedResult;
    
    @SneakyThrows(SQLException.class)
    @Override
    public String getColumnName(final int pos) {
        return queryResultSample.getMetaData().getColumnName(pos);
    }
    
    @Override
    public int getColumnIndex(final String column) {
        return columnLabelAndIndexMap.get(column);
    }
    
    @SneakyThrows(SQLException.class)
    @Override
    public Object getValue(final int pos) {
        return mergedResult.getValue(pos - 1, Object.class);
    }
    
    @Override
    public Tuple addValue(final Object value) {
        throw new UnsupportedOperationException();
    }
    
    @SneakyThrows(SQLException.class)
    @Override
    public int size() {
        return queryResultSample.getMetaData().getColumnCount();
    }
    
    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public List<Class<?>> types() {
        throw new UnsupportedOperationException();
    }
}
