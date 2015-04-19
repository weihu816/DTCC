package cn.ict.dtcc.server.dao;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Record {

	private String table;
    private String key;
    private long version = 0;
    Map<String, String> values;
    private boolean dirty;
    private String outstanding = null;

    public Record(String table, String key) {
        values = new ConcurrentHashMap<String, String>();
//        values.put(Database.DELETE_FIELD, Database.EXIST_STRING);
        this.setTable(table);
        this.setKey(key);
    }
    
    public Record(String table, String key, int v) {
        values = new ConcurrentHashMap<String, String>();
//        values.put(Database.DELETE_FIELD, Database.EXIST_STRING);
        this.setTable(table);
        this.setKey(key);
        this.setVersion(v);
    }
    
    public Record(String table, String key, Map<String, String> values) {
        this.values = values;
//        values.put(Database.DELETE_FIELD, Database.EXIST_STRING);
    	this.setTable(table);
        this.setKey(key);
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
    
    public void setValues(Map<String, String> values) {
		this.values.putAll(values);
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
	
	public void setOutstanding(String outstanding) {
		this.outstanding = outstanding;
	}

	// setter
	public void setTable(String table) {
		this.table = table;
	}
	
	public void setKey(String key) {
		this.key = key;
		values.put("key", key);
	}
	
	public void put(String name, String value) {
		values.put(name, value);
	}
	
	public void putAll(Map<String, String> newValues) {
		values.putAll(newValues);
	}

	public String getOutstanding() {
		return outstanding;
	}
	
//	public boolean isDeleted() {
//		return (values.get("__DELETE__") != null) && values.get("__DELETE__").equals("__DELETE__");
//	}
	
}
