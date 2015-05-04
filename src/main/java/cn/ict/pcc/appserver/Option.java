package cn.ict.pcc.appserver;

import java.util.List;

public class Option {

    private String table;
    private String key;
    private List<String> names;
    private List<String> values;

    public Option(String table, String key, List<String> names, List<String> values) {
    	this.table = table;
        this.key = key;
        this.names = names;
        this.values = values;
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


}
