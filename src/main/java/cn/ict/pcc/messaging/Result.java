package cn.ict.pcc.messaging;

import java.util.Map;


public class Result {

    private String table;
    private String key;
    private Map<String, String> values;
    private boolean deleted = false;

    public Result(String table, String key, Map<String, String> values) {
    	this.table = table;
        this.key = key;
        this.setValues(values);
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
    
    public boolean isDeleted() {
        return deleted;
    }
    
    // setter
    public void setValues(Map<String, String> values) {
		this.values = values;
	}

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }
	
}
