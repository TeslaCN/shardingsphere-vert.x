package icu.wwj.shardingsphere.vertx;

import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

@RequiredArgsConstructor
public class ShardingSphereUpdateRowSet implements RowSet<Row> {
    
    private final int updated;
    
    @Override
    public RowIterator<Row> iterator() {
        return new RowIterator<Row>() {
            
            @Override
            public boolean hasNext() {
                return false;
            }
            
            @Override
            public Row next() {
                throw new NoSuchElementException();
            }
        };
    }
    
    @Override
    public int rowCount() {
        return updated;
    }
    
    @Override
    public List<String> columnsNames() {
        return Collections.emptyList();
    }
    
    @Override
    public List<ColumnDescriptor> columnDescriptors() {
        return Collections.emptyList();
    }
    
    @Override
    public int size() {
        return 0;
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
        return this;
    }
}
