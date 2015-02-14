package cn.ict.rococo.server.dao;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Record {
	
	private Map<String,String> column = new ConcurrentHashMap<String, String>();

	public Map<String,String> getColumn() {
		return column;
	}

	public void setColumn(Map<String,String> column) {
		this.column = column;
	}
	
	public String get(String name) {
		return column.get(name);
	}
	
	public String put(String name, String value) {
		return column.put(name, value);
	}
	
//	public String replace(String name, String value) {
//		return column.replace(name, value);
//	}
	
}
