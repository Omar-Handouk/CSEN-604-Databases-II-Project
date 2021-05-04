import base.SQLTerm;

import java.util.Hashtable;

public class Main {

    public static void main(String[] args) {
        DBApp dbApp = new DBApp();

        Hashtable<String, Object> hashtable = new Hashtable<>();

        int phase = 4; // 0-Create Table, 1-Insertions, 2-Update, 3-Delete

        if (phase == 0) {
            String tableName = "Test";
            String clusteringKey = "id";

            Hashtable<String, String> colType = new Hashtable<>();
            colType.put("id", "java.lang.Integer");
            colType.put("name", "java.lang.String");
            colType.put("gpa", "java.lang.Double");

            Hashtable<String, String> colMin = new Hashtable<>();
            colMin.put("id", "0");
            colMin.put("name", "a");
            colMin.put("gpa", "0.0");

            Hashtable<String, String> colMax = new Hashtable<>();
            colMax.put("id", "1000000");
            colMax.put("name", "zzzzzzzzzzzzzzzzzzzz");
            colMax.put("gpa", "4.0");

            try {
                dbApp.createTable(tableName, clusteringKey, colType, colMin, colMax);
            } catch (DBAppException e) {
                e.printStackTrace();
            }
        } else if (phase == 1) {
            try {
                hashtable.put("id", 1);
                hashtable.put("name", "Test Name 1");
                hashtable.put("gpa", 0.1);
                dbApp.insertIntoTable("Test", hashtable);

                hashtable.clear();
                hashtable.put("id", 2);
                hashtable.put("name", "Test Name 2");
                hashtable.put("gpa", 0.2);
                dbApp.insertIntoTable("Test", hashtable);

                hashtable.clear();
                hashtable.put("id", 3);
                hashtable.put("name", "Test Name 3");
                hashtable.put("gpa", 0.3);
                dbApp.insertIntoTable("Test", hashtable);

                hashtable.clear();
                hashtable.put("id", 4);
                hashtable.put("name", "jacob");
                hashtable.put("gpa", 0.4);
                dbApp.insertIntoTable("Test", hashtable);

            } catch (DBAppException e) {
                e.printStackTrace();
            }
        } else if (phase == 2) {
            try {

                hashtable.put("name", "Test Name Changed #2");
                hashtable.put("gpa", 4.5);

                dbApp.updateTable("Test", "3", hashtable);
            } catch (DBAppException e) {
                e.printStackTrace();
            }
        } else if (phase == 3) {
            try {
                hashtable.put("name", "jacob");
                hashtable.put("gpa", 0.4);
                dbApp.deleteFromTable("Test", hashtable);
            } catch (DBAppException e) {
                e.printStackTrace();
            }
        } else if (phase == 4) {
            SQLTerm[] arrSQLTerms;
            arrSQLTerms = new SQLTerm[2];

            arrSQLTerms[0] = new SQLTerm("Test", "name", "=", "Test Name 1");
            arrSQLTerms[1] = new SQLTerm("Test", "gpa", "=", 0.1);

            try {
                dbApp.selectFromTable(arrSQLTerms, null);
            } catch (DBAppException e) {
                e.printStackTrace();
            }
        }
    }
}
