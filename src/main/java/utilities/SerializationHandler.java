package utilities;

import base.DBAppException;
import structures.Index;
import structures.Page;
import structures.Table;

import java.io.*;

public class SerializationHandler {

    public static final String DATAPATH = "src/main/resources/data";

    public static void saveTable(Table table) {
        checkDataPath();

        String tableDir = DATAPATH + "/" + table.getTableName();

        File dir = new File(tableDir);

        if (!dir.exists()) {
            dir.mkdirs();
        }

        try {
            FileOutputStream fileOut = new FileOutputStream(tableDir + "/" + table.getTableName() + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);

            out.writeObject(table);

            out.close();
            fileOut.close();

        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    public static void savePage(String tableName, Page page) {
        String tableDir = DATAPATH + "/" + tableName;

        try {
            FileOutputStream fileOut = new FileOutputStream(tableDir + "/" + page.getPageNumber() + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);

            out.writeObject(page);

            out.close();
            fileOut.close();
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    public static void saveIndex(String tableName, Index index) {
        String tableDir = DATAPATH + "/" + tableName;

        StringBuilder indexName = new StringBuilder();
        boolean first = true;
        for (String col : index.getColNames()) {
            if (first) {
                first = false;
            } else {
                indexName.append('-');
            }

            indexName.append(col);
        }

        indexName.append(".class");

        try {
            FileOutputStream fileOut = new FileOutputStream(tableDir + "/" + indexName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);

            out.writeObject(index);

            out.close();
            fileOut.close();
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }


    public static Table loadTable(String tableName) throws DBAppException {
        String tableDir = DATAPATH + "/" + tableName;

        File dir = new File(tableDir);

        if (!dir.exists()) {
            throw new DBAppException("Table Directory not found");
        }

        Table table = null;

        try {
            FileInputStream fileIn = new FileInputStream(tableDir + "/" + tableName + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);

            table = (Table) in.readObject();

            in.close();
            fileIn.close();

        } catch (IOException | ClassNotFoundException exception) {
            exception.printStackTrace();
        }

        return table;
    }

    public static Page loadPage(String tableName, int pageNumber) {
        String tableDir = DATAPATH + "/" + tableName;

        Page page = null;

        try {
            FileInputStream fileIn = new FileInputStream(tableDir + "/" + pageNumber + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);

            page = (Page) in.readObject();

            in.close();
            fileIn.close();
        } catch (IOException exception) {
            exception.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return page;
    }

    public static Index loadIndex(String tableName, String[] cols) {
        String tableDir = DATAPATH + "/" + tableName;

        StringBuilder indexName = new StringBuilder();
        boolean first = true;
        for (String col : cols) {
            if (first) {
                first = false;
            } else {
                indexName.append('-');
            }

            indexName.append(col);
        }

        indexName.append(".class");

        Index index = null;

        try {
            FileInputStream fileIn = new FileInputStream(tableDir + "/" + indexName);
            ObjectInputStream in = new ObjectInputStream(fileIn);

            index = (Index) in.readObject();

            in.close();
            fileIn.close();
        } catch (IOException exception) {
            exception.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return index;
    }

    public static void deletePage(String tableName, int pageNumber) {
        String tableDir = DATAPATH + "/" + tableName;
        String pageDir = tableDir + "/" + pageNumber + ".class";

        File file = new File(pageDir);

        if (file.delete()) {
            System.out.printf("Deleted %s's page #%d\n", tableName, pageNumber);
        } else {
            System.out.println("Failed to delete page");
        }
    }

    public static void deleteIndex(String tableName, String[] cols) throws DBAppException{
        String tableDir = DATAPATH + "/" + tableName;

        StringBuilder indexName = new StringBuilder();
        boolean first = true;
        for (String col : cols) {
            if (first) {
                first = false;
            } else {
                indexName.append('-');
            }

            indexName.append(col);
        }

        indexName.append(".class");

        File file = new File(tableDir + "/" + indexName);

        if (file.delete()) {
            System.out.printf("Delete index: %s successfully", indexName);
        } else {
            throw new DBAppException("Index does not exist");
        }
    }

    public static boolean checkDataPath() { // True-exists
        File dir = new File(DATAPATH);

        if (dir.exists()) {
            return true;
        }

        dir.mkdirs();
        return false;
    }
}
