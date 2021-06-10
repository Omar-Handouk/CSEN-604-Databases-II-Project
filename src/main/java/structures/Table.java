package structures;

import base.DBAppException;
import base.SQLTerm;
import utilities.ConfigureDB;
import utilities.MetadataHandler;
import utilities.SerializationHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {

    public static final String DEFAULT_STRING_VALUE = "z%C*F-JaNdRgUjXn2r5u8x/A?D(G+KbPeShVmYp3s6v9y$B&E)H@McQfTjWnZr4t7w!z%C*F-JaNdRgUkXp2s5v8x/A?D(G+KbPeShVmYq3t6w9z$B&E)H@McQfTjWnZr4u7x!A%D*F-JaNdRgUkXp2s5v8y/B?E(H+KbPeShVmYq3t6w9z$C&F)J@NcQfTjWnZr4u7x!A%D*G-KaPdSgUkXp2s5v8y/B?E(H+MbQeThWmYq3t6w9z$C&F)J@NcRfUjXn2r4u7x!A%D*G-KaPdSgVkYp3s6v8y/B?E(H+MbQeThWmZq4t7w!z$C&F)J@NcRfUjXn2r5u8x/A?D(G-KaPdSgVkYp3s6v9y$B&E)H@MbQeThWmZq4t7w!z%C*F-JaNdRfUjXn2r5u8x/A?D(G+KbPeShVkYp3s6v9y$B&E)H@McQfTjWnZq4t7w!z%C*F-JaNdRgUkXp2s5u8x/A?D(G+KbPeShVmYq3t6w9y$B&E)H@McQfTjWnZr4u7x";

    private static final long serialVersionUID = 4248559398412576914L;

    private final String tableName;
    private final String clusterKey;
    private final String clusterKeyType;

    private final Vector<String> colNames;
    private Hashtable<String, Hashtable> metadata;

    public Table(String tableName, String clusterKey, Hashtable<String, String> colTypes,
                 Hashtable<String, String> colMin, Hashtable<String, String> colMax) {

        this.tableName = tableName;
        this.clusterKey = clusterKey;
        this.clusterKeyType = colTypes.get(clusterKey);

        colNames = new Vector<>();
        colNames.addAll(colTypes.keySet());

        this.metadata = new Hashtable<>();

        for (String col : colNames) {
            Hashtable<String, Object> colDesc = new Hashtable<>();

            colDesc.put("type", colTypes.get(col));
            colDesc.put("cluster", col.compareTo(clusterKey) == 0);
            colDesc.put("index", false); // Default is false
            colDesc.put("min", parseValue(colTypes.get(col), colMin.get(col)));
            colDesc.put("max", parseValue(colTypes.get(col), colMax.get(col)));

            metadata.put(col, colDesc);
        }

        MetadataHandler.appendToMetaData(tableName, colNames, metadata);
        SerializationHandler.saveTable(this);
    }

    public void insert(Hashtable<String, Object> tuple) {
        int[] pagesEnum = null;
        try {
            pagesEnum = getPages();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        Hashtable<String, String> config = ConfigureDB.loadConfig();

        int pageLength = Integer.parseInt(config.get("MaximumRowsCountinPage"));

        Page page;

        if (pagesEnum.length == 0) {
            page = new Page(clusterKey, clusterKeyType, pageLength, 0, tableName);
        } else {
            page = null;

            // Search for the first empty page
            for (int pageNumber : pagesEnum) {
                page = SerializationHandler.loadPage(tableName, pageNumber);

                if (page.getPageMaxLength() <= page.getNumberOfEntries()) { // Page is full
                    page = null;
                } else {
                    break;
                }
            }

            if (page == null) { // If no empty pages
                int pageNumber = getFirstAvailablePageNumber(pagesEnum);
                page = new Page(clusterKey, clusterKeyType, pageLength, pageNumber, tableName);
            }

        }

        page.insert(tuple);
        SerializationHandler.savePage(tableName, page);
    }

    public void update(String clusteringKeyValue, Hashtable<String, Object> tuple) throws DBAppException {
        int[] pagesEnum = null;
        try {
            pagesEnum = getPages();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Page page = null;
        boolean found = false;

        assert pagesEnum != null;
        for (int pageNumber : pagesEnum) {
            page = SerializationHandler.loadPage(tableName, pageNumber);
            int entryIdx = page.checkIfEntryExists(clusteringKeyValue);

            if (entryIdx != -1) {
                page.update(entryIdx, tuple);
                found = true;
                break;
            }
        }

        if (!found) {
            throw new DBAppException("Entry not found");
        }

        SerializationHandler.savePage(tableName, page);
    }

    public void delete(Hashtable<String, Object> colValues) {
        int[] pagesEnum = null;
        try {
            pagesEnum = getPages();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }

        Page page = null;

        assert pagesEnum != null;
        for (int pageNumber : pagesEnum) {
            page = SerializationHandler.loadPage(tableName, pageNumber);
            page.delete(metadata, colValues);

            if (page.getNumberOfEntries() == 0) {
                SerializationHandler.deletePage(tableName, pageNumber);
            } else {
                SerializationHandler.savePage(tableName, page);
            }
        }
    }

    public Vector<Entry> select(SQLTerm query) {
        int[] pagesEnum = null;
        try {
            pagesEnum = getPages();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String colName = query.get_strColumnName();
        String colType = (String) metadata.get(colName).get("type");
        Object colValue = query.get_objValue();
        String colOperator = query.get_strOperator();

        Page page = null;

        Vector<Entry> result = new Vector<>();

        assert pagesEnum != null;
        for (int pageNumber : pagesEnum) {
            page = SerializationHandler.loadPage(tableName, pageNumber);

            result.addAll(page.select(colName, colType, colValue, colOperator));
        }

        return result;
    }

    public int[] getPages() throws FileNotFoundException {
        String tableDir = SerializationHandler.DATAPATH + "/" + tableName;

        File dir = new File(tableDir);

        if (!dir.exists()) {
            throw new FileNotFoundException("Table not found");
        }

        String[] fileNames = dir.list();

        Vector<Integer> pages = new Vector<>();

        for (String fileName : fileNames) {
            if (fileName.contains(tableName) || fileName.contains("Index")) {
                continue;
            }

            String[] split = fileName.split("\\.");

            pages.add(Integer.parseInt(split[0]));
        }

        int[] pageEnum = new int[pages.size()];
        for (int i = 0; i < pages.size(); ++i) pageEnum[i] = pages.get(i);

        Arrays.sort(pageEnum);

        return pageEnum;
    }
    //------------STATIC METHODS------------
    public static Object parseValue(String type, String val) {
        switch (type) {
            case "java.lang.Integer":
                return Integer.parseInt(val);
            case "java.lang.Long":
                return Long.parseLong(val);
            case "java.lang.Double":
                return Double.parseDouble(val);
            case "java.lang.Boolean":
                return Boolean.parseBoolean(val.toLowerCase());
            case "java.lang.String":
            case "java.util.Date":
                return val;
        }

        return null;
    }

    public static String stringValue(String type, Object val) {
        switch (type) {
            case "java.lang.Integer":
                return Integer.toString((int) val);
            case "java.lang.Long":
                return Long.toString((long) val);
            case "java.lang.Double":
                return Double.toString((double) val);
            case "java.lang.Boolean":
                return Boolean.toString((boolean) val);
            case "java.lang.String":
            case "java.util.Date":
                return ("\"" + val + "\"");
        }

        return null;
    }

    public static int compareKeys(String keyType, String key1, Object key2) {
        switch (keyType) {
            case "java.lang.Integer":
                return Integer.compare(Integer.parseInt(key1), (int) key2);
            case "java.lang.Long":
                return Long.compare(Long.parseLong(key1), (long) key2);
            case "java.lang.Double":
                return Double.compare(Double.parseDouble(key1), (double) key2);
            case "java.lang.Boolean":
                return Boolean.compare(Boolean.parseBoolean(key1.trim().toLowerCase()), (boolean) key2);
            case "java.lang.String":
                return key1.compareTo((String) key2);
            case "java.util.Date":
                LocalDate date1 = LocalDate.parse(key1);
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                String date2Formatted = format.format((Date) key2);
                LocalDate date2 = LocalDate.parse(date2Formatted);

                return date1.compareTo(date2);
        }

        return -1;
    }

    public static int compareKeysObjects(String keyType, Object key1, Object key2) {
        switch (keyType) {
            case "java.lang.Integer":
                return Integer.compare((int) key1, (int) key2);
            case "java.lang.Long":
                return Long.compare((long) key1, (long) key2);
            case "java.lang.Double":
                return Double.compare((double) key1, (double) key2);
            case "java.lang.Boolean":
                return Boolean.compare((boolean) key1, (boolean) key2);
            case "java.lang.String":
                return ((String) key1).compareTo((String) key2);
            default: // Dates
                return ((Date) key1).compareTo((Date) key2);
        }
    }

    public static Object defaultValues(String colType) {
        switch (colType) {
            case "java.lang.Integer":
                return Integer.MIN_VALUE;
            case "java.lang.Long":
                return Long.MIN_VALUE;
            case "java.lang.Double":
                return Double.MIN_VALUE;
            case "java.lang.Boolean":
                return false;
            case "java.lang.String":
                return DEFAULT_STRING_VALUE;
            case "java.util.Date":
                return "9999-12-31";
        }

        return null;
    }

    private static int getFirstAvailablePageNumber(int[] pagesEnum) {
        int maxNumber = pagesEnum[pagesEnum.length - 1]; // Get the last number which is the biggest, cuz sorting

        int i;
        for (i = 0; i <= maxNumber; ++i) {
            if (Arrays.binarySearch(pagesEnum, i) < 0) {
                break;
            }
        }

        return i;
    }
    //--------------GETTERS---------------
    public String getTableName() {
        return tableName;
    }

    public String getClusterKey() {
        return clusterKey;
    }

    public String getClusterKeyType() {
        return clusterKeyType;
    }

    public Vector<String> getColNames() {
        return colNames;
    }

    public Hashtable<String, Hashtable> getMetadata() {
        return metadata;
    }

    public void setMetadata(Hashtable<String, Hashtable> metadata) {
        this.metadata = metadata;
    }
}
