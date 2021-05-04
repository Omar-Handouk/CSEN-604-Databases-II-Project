package structures;

import java.io.Serializable;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
    private static final long serialVersionUID = -1400001927648513731L;

    private final int pageNumber;

    private final String clusterKey;
    private final String clusterKeyType;

    private final int pageMaxLength;

    private final Vector<Entry> page;

    public Page(String clusterKey, String clusterKeyType, int pageMaxLength, int pageNumber) {
        this.clusterKey = clusterKey;
        this.clusterKeyType = clusterKeyType;
        this.pageMaxLength = pageMaxLength;
        this.pageNumber = pageNumber;

        page = new Vector<>(pageMaxLength);

    }

    public void insert(Hashtable<String, Object> tuple) {
        Entry entry = new Entry(clusterKeyType, tuple.get(clusterKey), tuple);
        page.add(entry);
        Collections.sort(page);
    }

    public void update(int entryIdx, Hashtable<String, Object> tuple) {
        Entry entry = page.get(entryIdx);

        Hashtable<String, Object> data = entry.getData();

        for (String k : tuple.keySet()) {
            data.put(k, tuple.get(k));
        }

        entry.setData(data);
    }

    public void delete(Hashtable<String, Hashtable> metadata, Hashtable<String, Object> colValues) {
        Vector<Entry> removables = new Vector<>();

        for (Entry entry : page) {

            boolean validDeletion = true;

            for (String key : colValues.keySet()) {
                String keyType = (String) metadata.get(key).get("type");

                validDeletion = validDeletion && (Table.compareKeysObjects(keyType, entry.getData().get(key), colValues.get(key)) == 0);
            }

            if (validDeletion) {
                removables.add(entry);
            }
        }

        for (Entry entry : removables) {
            page.remove(entry);
        }
    }

    public Vector<Entry> select(String colName, String colType, Object colValue, String colOperator) {
        Vector<Entry> result = new Vector<>();

        for (Entry entry : page) {
           Object entryValue = entry.getData().get(colName);
           int compareValue = Table.compareKeysObjects(colType, entryValue, colValue);

           if (checkOperator(compareValue, colOperator)) {
               result.add(entry);
           }
        }

        return result;
    }

    public int checkIfEntryExists(String key) {
        for (int i = 0; i < page.size(); ++i) {
            if (Table.compareKeys(clusterKeyType, key, page.get(i).getKey()) == 0) {
                return i;
            }
        }

        return -1;
    }

    public static boolean checkOperator(int compareValue, String operator) {
        switch (operator) {
            case ">": return compareValue > 0;
            case ">=": return compareValue >= 0;
            case "<": return compareValue < 0;
            case "<=": return compareValue <= 0;
            case "=": return compareValue == 0;
            default: return compareValue != 0;
        }
    }

    public Vector<Entry> getPage() {
        return page;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public int getPageMaxLength() {
        return pageMaxLength;
    }

    public int getNumberOfEntries() {
        return page.size();
    }
}
