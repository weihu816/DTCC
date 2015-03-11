package cn.ict.rcc.server.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.rcc.exception.RococoException;

/**
 * A implementation of Memory Database
 * 
 * @author Wei Hu
 */

public class MemoryDB {

	private static final Log LOG = LogFactory.getLog(MemoryDB.class);

	private Map<String, ConcurrentHashMap<String, Record>> db = new ConcurrentHashMap<String, ConcurrentHashMap<String, Record>>();
	private Map<String, ConcurrentHashMap<String, List<String>>> secondaryIndex = new ConcurrentHashMap<String, ConcurrentHashMap<String, List<String>>>();
	private Map<String, List<String>> secondaryIndexInfo = new ConcurrentHashMap<String, List<String>>();
	
	public void check(String table) {
		if (!db.containsKey(table)) {
			db.put(table, new ConcurrentHashMap<String, Record>());
		}
	}

	public void init() {

	}

	/* Basic read */
	public HashMap<String, String> read(String table, String key,
			List<String> names) {
		check(table);
		Record record = db.get(table).get(key);
		if (record != null) {
			HashMap<String, String> map = new HashMap<String, String>();
			for (String name : names) {
				if (record.get(name) != null) {
					map.put(name, record.get(name));
				}
			}
			return map;
		}
		return null;
	}
	
	public List<Map<String, String>> read_secondaryIndex(String table, String key,
			List<String> names, boolean isAll) {
		check(table);
		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
		if (!isAll) {
			String primaryKey = secondaryIndex.get(table).get(key).get(0);
			list.add(read(table, primaryKey, names));
		} else {
			for(String primaryKey : secondaryIndex.get(table).get(key)) {
				list.add(read(table, primaryKey, names));
			}
		}
		return list;
		
	}

	public boolean write(String table, String key, List<String> names, List<String> values) {

		check(table);
		if (names == null || values == null || values.size() != names.size()) {
			return false;
		}
		int size = names.size();
		Record r = db.get(table).get(key);
		if (r == null) {
			r = new Record();
			db.get(table).put(key, r);
		}
		for (int i = 0; i < size; i++) {
			r.put(names.get(i), values.get(i));
		}
		// maintain secondary index
		if (secondaryIndexInfo.containsKey(table)) {
			StringBuffer stringBuffer = new StringBuffer();
			List<String> indexList =  secondaryIndexInfo.get(table);
			for(int i = 0; i < indexList.size() - 1; i++) {
				stringBuffer.append(r.get(indexList.get(i)));
				stringBuffer.append("_");
			}
			stringBuffer.append(r.get(indexList.get(indexList.size() - 1)));
			String indexKey = stringBuffer.toString();
			List<String> promaryKeyList = secondaryIndex.get(table).get(key);
			if (promaryKeyList == null) {
				promaryKeyList = new ArrayList<String>();
				secondaryIndex.get(table).put(key, promaryKeyList);
			}
			promaryKeyList.add(indexKey);
		}
		return true;
	}
	
	public boolean createSecondaryIndex(String table, List<String> fields) {
		secondaryIndexInfo.put(table, fields);
		secondaryIndex.put(table, new ConcurrentHashMap<String, List<String>>());
		return true;
	}
	
	public boolean addInteger(String table, String key, List<String> names, List<String> values) {

		check(table);
		if (names == null || values == null || values.size() != names.size()) {
			return false;
		}
		int size = names.size();
		Record r = db.get(table).get(key);
		if (r == null) {
			throw new RococoException("No such element table:" + table + " key: "+ key);
		}
		for (int i = 0; i < size; i++) {
			String new_value = String.valueOf(Integer.parseInt(r.get(names.get(i))) + Integer.parseInt(values.get(i)));
			r.put(names.get(i), new_value);
LOG.info("Table: " + table + " Key: " + key + " Add: " + names.get(i) + values.get(i));
		}
		return true;
	}
	
	public boolean delete(String table, String key) {
		Record r = db.get(table).get(key);
		if (secondaryIndexInfo.containsKey(table)) {
			StringBuffer stringBuffer = new StringBuffer();
			List<String> indexList =  secondaryIndexInfo.get(table);
			for(int i = 0; i < indexList.size() - 1; i++) {
				stringBuffer.append(r.get(indexList.get(i)));
				stringBuffer.append("_");
			}
			stringBuffer.append(r.get(indexList.get(indexList.size() - 1)));
			String indexKey = stringBuffer.toString();
			secondaryIndex.get(table).get(indexKey).remove(key);
		}
		db.get(table).remove(key);
		return true;
	}

}
