package cn.ict.occ.appserver;

import java.util.List;

public class Accept {

    private String transactionId;
    private String table;
    private String key;
    private long oldVersion;
    private List<String> names;
    private List<String> values;

    public Accept(String transactionId, Option option) {
        this.transactionId = transactionId;
        this.table = option.getTable();
        this.key = option.getKey();
        this.oldVersion = option.getOldVersion();
        this.names = option.getNames();
        this.values = option.getValues();
    }

    public Accept(String transactionId, String table, String key,
                  long oldVersion, List<String> names, List<String> values) {
        this.transactionId = transactionId;
        this.table = table;
        this.key = key;
        this.oldVersion = oldVersion;
        this.names = names;
        this.values = values;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getTable() {
        return table;
    }
    
    public String getKey() {
        return key;
    }

    public long getOldVersion() {
        return oldVersion;
    }

    public List<String> getNames() {
        return names;
    }
    
    public List<String> getValues() {
        return values;
    }

    public String toString() {
        return "table=" + table + " key=" + key + "; oldVersion=" + oldVersion + "; txn=" + transactionId;
    }
}
