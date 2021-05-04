package utilities;

import structures.Table;

import java.io.*;
import java.util.*;

public class MetadataHandler {

    private static String metadataPath = "src/main/resources/metadata.csv";
    private static Vector<String> tables = null;


    public static Hashtable<String, Hashtable> parseCSV() {
        tables = getTables();

        Hashtable<String, Hashtable> metadata = new Hashtable<>();

        for (String table : tables) {
            Hashtable<String, Hashtable> tb = new Hashtable<>();
            metadata.put(table, tb);
        }

        Scanner sc;

        try {
            sc = new Scanner(new File(metadataPath));

            sc.nextLine(); // Dispose of CSV header

            while (sc.hasNext()) {
                String[] split = sc.nextLine().split(",");
                for (int i = 0; i < split.length; ++i) split[i] = split[i].trim().replace("\"", "");

                String tableName = split[0];

                Hashtable<String, Hashtable> tableMetadata = metadata.get(tableName);

                String colName = split[1];

                String colType = split[2];
                Boolean colClusterKeying = Boolean.parseBoolean(split[3].toLowerCase());
                Boolean colIndexed = Boolean.parseBoolean(split[4].toLowerCase());
                Object colMin = Table.parseValue(colType, split[5]);
                Object colMax = Table.parseValue(colType, split[6]);

                Hashtable<String, Object> data = new Hashtable<>();
                data.put("type", colType);
                data.put("cluster", colClusterKeying);
                data.put("index", colIndexed);
                data.put("min", colMin);
                data.put("max", colMax);

                tableMetadata.put(colName, data);
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }

        return metadata;
    }

    private static Vector<String> getTables () {
        Vector<String> names = new Vector<>();
        HashSet<String> existing = new HashSet<>();

        Scanner sc;

        try {
            sc = new Scanner(new File(metadataPath));

            sc.nextLine(); // Dispose of CSV header

            while (sc.hasNext()) {
                String[] split = sc.nextLine().split(",");
                String tableName = split[0].trim();

                if (!existing.contains(tableName)) {
                    names.add(tableName);
                    existing.add(tableName);
                }
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }

        return names;
    }

    public static void appendToMetaData(String tableName, Vector<String> colNames,
                                        Hashtable<String, Hashtable> tableMetadata) {

        Writer out = null;

        try {
            out = new BufferedWriter(new FileWriter("src/main/resources/metadata.csv", true));

            for (int i = 0; i < colNames.size(); ++i) {
                StringBuilder line = new StringBuilder();

                Hashtable<String, Object> colMetadata = tableMetadata.get(colNames.get(i));

                String colType = (String) colMetadata.get("type");
                String isCluster = Boolean.toString((boolean) colMetadata.get("cluster"));
                String hasIndex = Boolean.toString((boolean) colMetadata.get("index"));
                String min = Table.stringValue(colType, colMetadata.get("min"));
                String max = Table.stringValue(colType, colMetadata.get("max"));

                line
                        .append(tableName)
                        .append(",")
                        .append(colNames.get(i))
                        .append(",")
                        .append(colType)
                        .append(",")
                        .append(isCluster)
                        .append(",")
                        .append(hasIndex)
                        .append(",")
                        .append(min)
                        .append(",")
                        .append(max)
                        .append("\n");

                out.append(line.toString());
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }

        if (out != null) {
            try {
                out.flush();
                out.close();
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }
    }
}
