package icu.wwj.shardingsphere.vertx;

import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.desc.ColumnDescriptor;

import java.util.Iterator;
import java.util.List;

public class ShardingSphereRowSet implements Iterable<Row>, SqlResult<RowSet<Row>> {
    
    private List<RowSet<Row>> rowSets;
    
    @Override
    public int rowCount() {
        return 0;
    }
    
    @Override
    public List<String> columnsNames() {
        return null;
    }
    
    @Override
    public List<ColumnDescriptor> columnDescriptors() {
        return null;
    }
    
    @Override
    public int size() {
        return 0;
    }
    
    @Override
    public <V> V property(final PropertyKind<V> propertyKind) {
        return null;
    }
    
    @Override
    public RowSet<Row> value() {
        return null;
    }
    
    @Override
    public SqlResult<RowSet<Row>> next() {
        return null;
    }
    
    @Override
    public Iterator<Row> iterator() {
        return null;
    }
}
