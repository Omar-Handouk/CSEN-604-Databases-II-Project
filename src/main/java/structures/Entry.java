package structures;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Hashtable;

public class Entry implements Serializable, Comparable<Entry> {
    private static final long serialVersionUID = 50776575695888995L;

    private String keyType;
    private Object key;

    private Hashtable<String, Object> data;

    public Entry(String keyType, Object key, Hashtable<String, Object> data) {
        this.keyType = keyType;
        this.key = key;
        this.data = data;
    }

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Hashtable<String, Object> getData() {
        return data;
    }

    public void setData(Hashtable<String, Object> data) {
        this.data = data;
    }

    @Override
    public int compareTo(Entry entry) {
        switch (keyType) {
            case "java.lang.Integer":
                return Integer.compare((int) this.key, (int) entry.key);
            case "java.lang.Long":
                return Long.compare((long) this.key, (long) entry.key);
            case "java.lang.Double":
                return Double.compare((double) this.key, (double) entry.key);
            case "java.lang.Boolean":
                return Boolean.compare((boolean) this.key, (boolean) entry.key);
            case "java.lang.String":
                return ((String) this.key).compareTo((String) entry.key);
            case "java.util.Date":
                LocalDate date1 = LocalDate.parse((String) this.key);
                LocalDate date2 = LocalDate.parse((String) entry.key);

                return date1.compareTo(date2);
        }
        return 0;
    }

    @Override
    public String toString() {
        return "Entry{" +
                "keyType='" + keyType + '\'' +
                ", key=" + key +
                '}';
    }
}
