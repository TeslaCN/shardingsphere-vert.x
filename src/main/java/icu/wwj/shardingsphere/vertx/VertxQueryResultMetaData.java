package icu.wwj.shardingsphere.vertx;

import io.vertx.sqlclient.desc.ColumnDescriptor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResultMetaData;

import java.util.List;

@RequiredArgsConstructor
public class VertxQueryResultMetaData implements QueryResultMetaData {
    
    private final List<ColumnDescriptor> columnDescriptors;
    
    /**
     * TODO Temporary solution. This field should not be appeared here.
     */
    @Getter
    private final int rowCount;
    
    @Override
    public int getColumnCount() {
        return columnDescriptors.size();
    }
    
    @Override
    public String getTableName(final int columnIndex) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String getColumnName(final int columnIndex) {
        return columnDescriptors.get(columnIndex - 1).name();
    }
    
    @Override
    public String getColumnLabel(final int columnIndex) {
        return columnDescriptors.get(columnIndex - 1).name();
    }
    
    @Override
    public int getColumnType(final int columnIndex) {
        return columnDescriptors.get(columnIndex - 1).jdbcType().getVendorTypeNumber();
    }
    
    @Override
    public String getColumnTypeName(final int columnIndex) {
        return columnDescriptors.get(columnIndex - 1).typeName();
    }
    
    @Override
    public int getColumnLength(final int columnIndex) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public int getDecimals(final int columnIndex) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isSigned(final int columnIndex) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isNotNull(final int columnIndex) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isAutoIncrement(final int columnIndex) {
        throw new UnsupportedOperationException();
    }
}
