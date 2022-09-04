package icu.wwj.shardingsphere.vertx;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResultMetaData;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RowSetUtil {
    
    @SneakyThrows(SQLException.class)
    public static Map<String, Integer> createColumnLabelAndIndexMap(final QueryResultMetaData queryResultMetaData) {
        Map<String, Integer> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int columnIndex = 1; columnIndex <= queryResultMetaData.getColumnCount(); columnIndex++) {
            result.put(queryResultMetaData.getColumnName(columnIndex), columnIndex);
        }
        return result;
    } 
}
