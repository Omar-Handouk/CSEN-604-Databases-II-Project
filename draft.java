for (Entry entry : page.getPage()) {
        if (compareKeys(clusterKeyType, clusteringKeyValue, entry.getKey()) == 0) {

        Hashtable<String, Object> data = entry.getData();

        for (String k : tuple.keySet()) {
        data.put(k, tuple.get(k));
        }

        entry.setData(data);
        found = true;
        break outer;
        }
        }