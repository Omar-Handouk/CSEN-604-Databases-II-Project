import base.DBAppException;
import base.SQLTerm;
import structures.Entry;
import structures.Index;
import structures.Table;
import utilities.MetadataHandler;
import utilities.SerializationHandler;

import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface{
    @Override
    public void init() {

    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax);
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        Hashtable<String, Hashtable> tableMetadata = MetadataHandler.parseCSV().get(tableName);

        new Index(tableMetadata, tableName, columnNames);
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        /*
         * Load Table
         * Load Metadata for the table
         * Assign metadata
         * Call Table.insert
         */
        try {
            Table table = SerializationHandler.loadTable(tableName);
            Hashtable<String, Hashtable> tableMeta = MetadataHandler.parseCSV().get(tableName);
            table.setMetadata(tableMeta);
            table.insert(colNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        try {
            Table table = SerializationHandler.loadTable(tableName);
            Hashtable<String, Hashtable> tableMeta = MetadataHandler.parseCSV().get(tableName);
            table.setMetadata(tableMeta);
            table.update(clusteringKeyValue, columnNameValue);
        } catch (DBAppException e) {
            throw e;
        }
    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        try {
            Table table = SerializationHandler.loadTable(tableName);
            Hashtable<String, Hashtable> tableMeta = MetadataHandler.parseCSV().get(tableName);
            table.setMetadata(tableMeta);
            table.delete(columnNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        String tableName = sqlTerms[0].get_strTableName();

        Queue<Vector<Entry>> results = new LinkedList<>();

        try {
            Table table = SerializationHandler.loadTable(tableName);
            Hashtable<String, Hashtable> tableMeta = MetadataHandler.parseCSV().get(tableName);
            table.setMetadata(tableMeta);

            for (SQLTerm query : sqlTerms) {
                results.add(table.select(query));
            }
        } catch (DBAppException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Hashtable<String, Object> changeDataToDateString(Hashtable<String, Hashtable> metadata,
                                                            Hashtable<String, Object> data) {
        for (String str : data.keySet()) {
            String colType = (String) metadata.get(str).get("type");

            if (data.get(str).getClass() == Date.class) {
                data.put(str, parseDateToString((Date) data.get(str)));
            }
        }

        return data;
    }

    public static String parseDateToString(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        return format.format(date);
    }
}
