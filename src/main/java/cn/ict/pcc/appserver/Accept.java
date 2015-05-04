package cn.ict.pcc.appserver;

import java.util.List;

public class Accept {

    private String transactionId;
    private String table;
    private String key;
    private List<String> names;
    private List<String> values;

    public Accept(String transactionId, Option option) {
        this.transactionId = transactionId;
        this.table = option.getTable();
        this.key = option.getKey();
        this.names = option.getNames();
        this.values = option.getValues();
    }

    public Accept(String transactionId, String table, String key,
                   List<String> names, List<String> values) {
        this.transactionId = transactionId;
        this.table = table;
        this.key = key;
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

    public List<String> getNames() {
        return names;
    }
    
    public List<String> getValues() {
        return values;
    }

    public String toString() {
        return "table=" + table + " key=" + key + "; txn=" + transactionId;
    }
}
