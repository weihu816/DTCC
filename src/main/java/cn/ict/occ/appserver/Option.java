package cn.ict.occ.appserver;

import java.util.List;

public class Option {

    private String table;
    private String key;
    private List<String> names;
    private List<String> values;
    private long oldVersion;

    public Option(String table, String key, List<String> names, List<String> values, long oldVersion) {
    	this.table = table;
        this.key = key;
        this.names = names;
        this.values = values;
        this.oldVersion = oldVersion;
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

    public long getOldVersion() {
        return oldVersion;
    }

}
