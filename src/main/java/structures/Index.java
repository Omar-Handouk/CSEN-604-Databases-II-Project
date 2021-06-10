package structures;

import base.DBAppException;
import utilities.SerializationHandler;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class Index implements Serializable {

    transient public static final int INTERVALS = 10;
    private static final long serialVersionUID = 5290445587578106696L;

    private final Hashtable<String, Hashtable> tableMetadata;
    private final Hashtable<String, Vector<Long>> rangeTable;
    private final String tableName;
    private final String[] colNames;

    private final Object[] grid;

    public Index(Hashtable<String, Hashtable> tableMetadata, String tableName, String[] colNames) {
        rangeTable = new Hashtable<>();

        this.tableMetadata = tableMetadata;
        this.tableName = tableName;
        this.colNames = colNames;

        this.constructRangeTable();
        grid = this.constructGrid(0);

        this.constructIndex();
        SerializationHandler.saveIndex(tableName, this);
    }

    private void constructRangeTable() {
        for (String colName : colNames) {
            Hashtable<String, Object> colMetadata = tableMetadata.get(colName);
            String colType = (String) colMetadata.get("type");

            Object colMin = colMetadata.get("min");
            Object colMax = colMetadata.get("max");

            long rangeMin = -1;
            long rangeMax = -1;
            long rangeStep = -1;

            if (colType.compareTo("java.lang.Integer") == 0 || colType.compareTo("java.lang.Double") == 0) {
                // If integer just get the min and max
                // If double floor the min, ceil the max and get the biggest bound
                rangeMin = colType.compareTo("java.lang.Integer") == 0 ? (new Integer((int) colMin)).longValue() : (new Double((Math.floor((double) colMin)))).longValue();
                rangeMax = colType.compareTo("java.lang.Integer") == 0 ? (new Integer((int) colMax)).longValue() : (new Double((Math.ceil((double) colMax)))).longValue();

                rangeStep = step(rangeMin, rangeMax, INTERVALS);
            } else if (colType.compareTo("java.lang.String") == 0) {
                rangeMin = stringNumericalValue((String) colMin);
                rangeMax = stringNumericalValue((String) colMax);

                rangeStep = step(rangeMin, rangeMax, INTERVALS);
            } else if (colType.compareTo("java.util.Date") == 0) {
                DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    Date minDate = formatter.parse((String) colMin);
                    Date maxDate = formatter.parse((String) colMax);

                    rangeMin = dateToSeconds(minDate);
                    rangeMax = dateToSeconds(maxDate);

                    rangeStep = step(rangeMin, rangeMax, INTERVALS);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            Vector<Long> range = new Vector<>();

            long current = rangeMin;
            for (int i = 1; i <= INTERVALS && current <= rangeMax; ++i) {
                range.add(current);

                current += rangeStep;
            }

            rangeTable.put(colName, range);
        }
    }

    private Object[] constructGrid(int level) {
        int dimension = rangeTable.get(colNames[level]).size();
        Object[] grid = new Object[dimension];

        if (level == colNames.length - 1) {
            for (int i = 0; i < grid.length; i++) {
                grid[i] = new Vector<BucketTuple>();
            }
        } else {
            for (int i = 0; i < grid.length; i++) {
                grid[i] = constructGrid(level + 1);
            }
        }

        return grid;
    }

    private void constructIndex() {
        try {
            Table table = SerializationHandler.loadTable(tableName);

            int[] pagesEnumeration = table.getPages();

            for (int pageNumber : pagesEnumeration) {
                Page page = SerializationHandler.loadPage(tableName, pageNumber);

                Vector<Entry> p = page.getPage();

                for (int i = 0; i < p.size(); i++) {
                    BucketTuple tuple = new BucketTuple(page.getPageNumber(), i);

                    this.insertIntoBucket(p.get(i), tuple);
                }
            }
        } catch (DBAppException | FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void indexOperation(IndexOperation operation, Entry oldEntry, Entry currentEntry,
                               BucketTuple oldTuple, BucketTuple currentTuple) throws DBAppException {
        if (operation == IndexOperation.UPDATE) {
            updateInIndex(oldEntry, currentEntry, oldTuple, currentTuple);
        }

        SerializationHandler.saveIndex(tableName, this);
    }

    public void indexOperation(IndexOperation operation, Entry entry, BucketTuple tuple) throws DBAppException {

        switch (operation) {
            case INSERT:
                insertIntoBucket(entry, tuple);
                break;
            case DELETE:
                deleteFromBucket(entry, tuple);
                break;
        }

        SerializationHandler.saveIndex(tableName, this);
    }

    public void insertIntoBucket(Entry entry, BucketTuple tuple) throws DBAppException {
        Hashtable<String, Object> data = entry.getData();

        int[] compoundIndexForGrid = getIndex(data);

        for (int i = 0; i < compoundIndexForGrid.length; i++) {
            if (compoundIndexForGrid[i] == -1) {
                throw new DBAppException("Invalid entry can not insert into index, does not comply with min-max range");
            }
        }

        insertIntoBucket(0, compoundIndexForGrid, grid, tuple);
    }

    public void insertIntoBucket(int level, int[] idx, Object grid, BucketTuple tuple) {
        Object[] tmp = (Object[]) grid;

        if (level == colNames.length - 1) {
            ((Vector<BucketTuple>) tmp[idx[level]]).add(tuple);
        } else {
            insertIntoBucket(level + 1, idx, tmp[idx[level]], tuple);
        }
    }

    public void updateInIndex(Entry oldEntry, Entry currentEntry, BucketTuple oldTuple, BucketTuple currentTuple) throws DBAppException { // oldTuple differs from current tuple in row number
        deleteFromBucket(oldEntry, oldTuple);
        insertIntoBucket(currentEntry, currentTuple);
    }

    public void deleteFromBucket(Entry entry, BucketTuple tuple) throws DBAppException {
        Hashtable<String, Object> data = entry.getData();

        int[] compoundIndexForGrid = getIndex(data);

        for (int i = 0; i < compoundIndexForGrid.length; i++) {
            if (compoundIndexForGrid[i] == -1) {
                throw new DBAppException("Invalid entry can not delete from index, does not comply with min-max range");
            }
        }

        Vector<BucketTuple> bucket = getBucket(compoundIndexForGrid);

        for (int i = 0; i < bucket.size(); i++) {
            if (bucket.get(i).getPageNumber() == tuple.getPageNumber()
                    && bucket.get(i).getRowNumber() == tuple.getRowNumber()) {
                bucket.remove(i);
                break;
            }
        }
    }

    private Vector<BucketTuple> getBucket(int[] idx) {
        return getBucket(0, idx, grid);
    }

    private Vector<BucketTuple> getBucket(int level, int[] idx, Object grid) {
        Object[] tmp = (Object[]) grid;

        if (level == colNames.length - 1) {
            return (Vector<BucketTuple>) tmp[idx[level]];
        }

        return getBucket(level + 1, idx, tmp[idx[level]]);
    }

    private int[] getIndex(Hashtable<String, Object> data) {
        int[] compoundIndexForGrid = new int[colNames.length];

        for (int i = 0; i < colNames.length; i++) {
            Object value = data.get(colNames[i]);

            // TODO: Problem as double need to be found, dont care enough to fix
            if (value.getClass() == Integer.class || value.getClass() == Long.class || value.getClass() == Double.class) {
                long v = value.getClass() == Integer.class ? (new Integer((int) value)).longValue()
                        : value.getClass() == Double.class ? (new Double((double) value)).longValue()
                        : (long) value;

                compoundIndexForGrid[i] = floorSearch(rangeTable.get(colNames[i]), v);
            } else if (value.getClass() == String.class) {
                compoundIndexForGrid[i] = floorSearch(rangeTable.get(colNames[i]), stringNumericalValue((String) value));
            } else if (value.getClass() == Date.class) {
                compoundIndexForGrid[i] = floorSearch(rangeTable.get(colNames[i]), dateToSeconds((Date) value));
            }
        }

        return compoundIndexForGrid;
    }


    /*
     * This method returns the accumulative sum of the ascii value of character of a string,
     * All strings are converted to lowercase, trimmed, and white spaces are removed
     */
    public static long stringNumericalValue(String str) {
        str = str.toLowerCase().trim().replaceAll(" ", "");

        long value = 0;
        for (char chr : str.toCharArray()) {
            value += chr;
        }

        return value;
    }

    public static long dateToSeconds(Date date) {
        return date.getTime() / 1000;
    }

    public static long step(long min, long max, long intervals) {
        return (long) Math.ceil(1f * (max - min + 1) / intervals);
    }

    public static int floorSearch(Vector<Long> range, long target) {
        if (range.get(range.size() - 1) <= target) {
            return range.size() - 1;
        }

        if (target < range.get(0)) {
            return -1;
        }

        int lowerIdx = 0;
        int upperIdx = range.size() - 1;
        int mid;

        while (lowerIdx <= upperIdx) {
            mid = lowerIdx + (upperIdx - lowerIdx) / 2;

            if (range.get(mid) == target) {
                return mid;
            }

            if (mid > 0 && range.get(mid - 1) <= target && target < range.get(mid)) {
                return mid - 1;
            }

            if (target < range.get(mid)) {
                upperIdx = mid - 1;
            }

            if (range.get(mid) < target) {
                lowerIdx = mid + 1;
            }
        }

        return -1;
    }

    public static Index[] getIndices(String tableName) {
        Vector<Index> indices = new Vector<>();

        String indicesPath = "src/main/resources/data/" + tableName;

        File folder = new File(indicesPath);
        File[] files = folder.listFiles();

        assert files != null;
        for (File file : files) {
            if (file.isFile() && file.getName().contains("Index")) {
                try {
                    FileInputStream fileIn = new FileInputStream(file);
                    ObjectInputStream in = new ObjectInputStream(fileIn);

                    indices.add((Index) in.readObject());

                    fileIn.close();
                    in.close();
                } catch (IOException | ClassNotFoundException exception) {
                    exception.printStackTrace();
                }
            }
        }

        return indices.toArray(new Index[indices.size()]);
    }

    public Hashtable<String, Vector<Long>> getRangeTable() {
        return rangeTable;
    }

    public String[] getColNames() {
        return colNames;
    }

    public Object[] getGrid() {
        return grid;
    }
}
