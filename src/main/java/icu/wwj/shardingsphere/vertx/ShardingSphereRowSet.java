package icu.wwj.shardingsphere.vertx;

import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResultMetaData;
import org.apache.shardingsphere.infra.merge.result.MergedResult;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ShardingSphereRowSet implements RowSet<Row> {
    
    private final QueryResult queryResultSample;
    
    private final Map<String, Integer> columnLabelAndIndexMap;
    
    private final MergedResult mergedResult;
    
    private final int size;
    
    private final RowIterator<Row> rowIterator;
    
    public ShardingSphereRowSet(final QueryResult queryResultSample, final Map<String, Integer> columnLabelAndIndexMap, final MergedResult mergedResult, final int size) {
        this.queryResultSample = queryResultSample;
        this.columnLabelAndIndexMap = columnLabelAndIndexMap;
        this.mergedResult = mergedResult;
        this.size = size;
        rowIterator = new RowIterator<Row>() {
            
            @SneakyThrows(SQLException.class)
            @Override
            public boolean hasNext() {
                return mergedResult.next();
            }
            
            @Override
            public Row next() {
                return new ShardingSphereRow(queryResultSample, columnLabelAndIndexMap, mergedResult);
            }
        };
    }
    
    @Override
    public int rowCount() {
        return 0;
    }
    
    @SneakyThrows(SQLException.class)
    @Override
    public List<String> columnsNames() {
        QueryResultMetaData metaData = queryResultSample.getMetaData();
        List<String> result = new ArrayList<>(metaData.getColumnCount());
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            result.add(metaData.getColumnName(i));
        }
        return result;
    }
    
    @Override
    public List<ColumnDescriptor> columnDescriptors() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public int size() {
        return size;
    }
    
    @Override
    public <V> V property(final PropertyKind<V> propertyKind) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public RowSet<Row> value() {
        return this;
    }
    
    @Override
    public RowSet<Row> next() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public RowIterator<Row> iterator() {
        return rowIterator;
    }
}
