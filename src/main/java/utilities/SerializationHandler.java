package utilities;

import structures.Page;
import structures.Table;

import java.io.*;

public class SerializationHandler {

    public static final String DATAPATH = "src/main/data";

    public static void saveTable(Table table) {
        checkDataPath();

        String tableDir = DATAPATH + "/" + table.getTableName();

        File dir = new File(tableDir);

        if (!dir.exists()) {
            dir.mkdirs();
        }

        try {
            FileOutputStream fileOut = new FileOutputStream(tableDir + "/" + table.getTableName() + ".ser");
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
            FileOutputStream fileOut = new FileOutputStream(tableDir + "/" + page.getPageNumber() + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);

            out.writeObject(page);

            out.close();
            fileOut.close();
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    public static Table loadTable(String tableName) throws FileNotFoundException{
        String tableDir = DATAPATH + "/" + tableName;

        File dir = new File(tableDir);

        if (!dir.exists()) {
            throw new FileNotFoundException("Table Directory not found");
        }

        Table table = null;

        try {
            FileInputStream fileIn = new FileInputStream(tableDir + "/" + tableName + ".ser");
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
            FileInputStream fileIn = new FileInputStream(tableDir + "/" + pageNumber + ".ser");
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

    public static void deletePage(String tableName, int pageNumber) {
        String tableDir = DATAPATH + "/" + tableName;
        String pageDir = tableDir + "/" + pageNumber + ".ser";

        File file = new File(pageDir);

        if (file.delete()) {
            System.out.printf("Deleted %s's page #%d\n", tableName, pageNumber);
        } else {
            System.out.println("Failed to delete page");
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
