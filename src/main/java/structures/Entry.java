package structures;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Date;
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
                return ((Date) this.key).compareTo((Date) entry.key);
        }
        return 0;
    }

    @Override
    public String toString() {

        StringBuilder out = new StringBuilder();

        out.append("Entry{" +
                "keyType='" + keyType + '\'' +
                ", key=" + key);

        for (String key : data.keySet()) {
            out.append(", ").append(key).append("=");

            Object value = data.get(key);
            if (value.getClass() == Integer.class) {
                out.append((int) value);
            } else if (value.getClass() == Long.class) {
                out.append((long) value);
            } else if (value.getClass() == Double.class) {
                out.append((double) value);
            } else if (value.getClass() == Boolean.class) {
                out.append((boolean) value);
            } else if (value.getClass() == String.class) {
                out.append((String) value);
            } else if (value.getClass() == Date.class) {
                out.append((Date) value);
            }
        }

        out.append('}');

        return out.toString();
    }
}
