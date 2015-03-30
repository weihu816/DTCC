package cn.ict.occ.server.dao;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Option {

    private static final Log log = LogFactory.getLog(Option.class);

    private String table;
    private String key;
    private Map<String, String> values;
    private long oldVersion;

    public Option(String table, String key, Map<String, String>  values, long oldVersion) {
    	this.table = table;
        this.key = key;
        this.values = values;
        this.oldVersion = oldVersion;
    }

    public String getTable() {
        return table;
    }
    
    public String getKey() {
        return key;
    }

    public Map<String, String>  getValues() {
        return values;
    }

    public long getOldVersion() {
        return oldVersion;
    }

}
