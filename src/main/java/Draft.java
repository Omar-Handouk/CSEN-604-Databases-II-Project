import mikera.randomz.Hash;
import structures.*;
import utilities.MetadataHandler;
import utilities.SerializationHandler;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Vector;

public class Draft {

    public static void main(String[] args) throws Exception{
        /*Hashtable<String, Hashtable> metadata = new Hashtable<>();
        metadata = MetadataHandler.parseCSV();

        String tableName = "students";

        Hashtable<String, Hashtable> colsMetadata = metadata.get(tableName);

        String[] cols = new String[]{"gpa", "first_name", "dob", "id"};

        Index index = new Index(colsMetadata, cols);*/

        String[] cols = {"id", "dob", "first_name"};

        String tableName = "students";

        DBApp dbApp = new DBApp();

        dbApp.createIndex(tableName, cols);

        /*Index index = (Index) SerializationHandler.loadIndex(tableName, cols);

        Page page = SerializationHandler.loadPage("students", 0);
        Vector<Entry> p = page.getPage();

        index.indexOperation(IndexOperation.DELETE, p.get(0), new BucketTuple(0, 0));*/
    }
}
