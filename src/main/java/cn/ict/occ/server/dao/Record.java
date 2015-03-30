package cn.ict.occ.server.dao;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Record extends Cacheable {

	private String table;
    private String key;
    private long version = 0;
    Map<String, String> values;
    private boolean dirty;

    public Record(String table, String key) {
    	this.setTable(table);
        this.setKey(key);
        values = new ConcurrentHashMap<String, String>();
    }
    
    public Record(String table, String key, Map<String, String> values) {
    	this.setTable(table);
        this.setKey(key);
        this.values = values;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    // getter
	public String getTable() {
		return table;
	}
	
	public String getKey() {
		return key;
	}

	public Map<String, String> getValues() {
		return values;
	}
	
	public String getValue(String name) {
		return values.get(name);
	}

	// setter
	public void setTable(String table) {
		this.table = table;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public void put(String name, String value) {
		values.put(name, value);
	}
	
	public void putAll(Map<String, String> newValues) {
		values.putAll(newValues);
	}
}
