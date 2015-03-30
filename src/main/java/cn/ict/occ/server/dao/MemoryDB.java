package cn.ict.occ.server.dao;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A implementation of Memory Database
 * 
 * @author Wei Hu
 */

public class MemoryDB {
	private static final Log LOG = LogFactory.getLog(MemoryDB.class);
	
	private Map<String, ConcurrentHashMap<String, Record>> db = new ConcurrentHashMap<String, ConcurrentHashMap<String, Record>>();
	private Map<String, ConcurrentHashMap<String, Set<String>>> secondaryIndex = new ConcurrentHashMap<String, ConcurrentHashMap<String, Set<String>>>();
	private Map<String, List<String>> secondaryIndexInfo = new ConcurrentHashMap<String, List<String>>();
	
	public void init() {

    }

    public void shutdown() {

    }
    
	public void createTables(List<String> tables) {
		for (String table: tables) {
			db.put(table, new ConcurrentHashMap<String, Record>());
		}
    }
	
	public Record get(String table, String key) {    	
        Record record = db.get(table).get(key);
        if (record == null) {
        	record = new Record(table, key);
        }
        return record;
    }
    
    public void put(Record record) {
    	String table = record.getTable();
    	String key = record.getKey();
     	Record record_read = db.get(table).get(key);
    	record_read.putAll(record.getValues());
    }
    
    // secondary index
    public boolean createSecondaryIndex(String table, List<String> fields) {
		LOG.debug("create secondaryIndex Table: " + table + " Fields: " + fields);
		secondaryIndexInfo.put(table, fields);
		secondaryIndex.put(table, new ConcurrentHashMap<String, Set<String>>());
		return true;
	}
    
//    // read from secondary index
//    public List<Map<String, String>> read_secondaryIndex(String table, String key,
//			List<String> names, boolean isAll) {
//		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
//		if (!isAll) {
//			Iterator<String> i = secondaryIndex.get(table).get(key).iterator();
//		
//			String primaryKey = i.next();
//			list.add(read(table, primaryKey, names));
//		} else {
//			for(String primaryKey : secondaryIndex.get(table).get(key)) {
//				list.add(read(table, primaryKey, names));
//			}
//		}
//		return list;
//	}
}
