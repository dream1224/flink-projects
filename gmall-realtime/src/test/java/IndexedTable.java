import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class IndexedTable {
    public static <T extends BaseBean, R extends BaseBean> Map<String, R> getIndexedTable(Map<String, T> indexMap, Map<String, R> indexedMap, String[] rowKey, String fieldName) throws Exception {
        HashSet<String> rowKeySet = new HashSet<>();
        HashMap<String, R> returnMap = new HashMap<>();
        for (Map.Entry<String, T> tEntry : indexMap.entrySet()) {
            String fieldValue = (String) tEntry.getValue().getValueVidFieldName(fieldName);
            String[] rowKeys = fieldValue.split(",");
            rowKeySet.addAll(Arrays.asList(rowKeys));
        }
        for (String s : rowKeySet) {
            returnMap.put(s, indexedMap.get(s));
        }
        return returnMap;
    }
}
