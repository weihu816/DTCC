package cn.ict.dtcc.server.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.dtcc.exception.DTCCException;
import cn.ict.dtcc.util.DTCCUtil;

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
	
    private Map<String,TransactionRecord> transactions = new ConcurrentHashMap<String, TransactionRecord>();

	public void init() {

    }

    public void shutdown() {

    }
    
	public void check(String table) {
	if (!db.containsKey(table)) {
		db.put(table, new ConcurrentHashMap<String, Record>());
	}
}
    
	public void createTables(List<String> tables) {
		for (String table: tables) {
			db.put(table, new ConcurrentHashMap<String, Record>());
		}
    }
	
	
	public TransactionRecord getTransactionRecord(String transactionId) {
        TransactionRecord record = transactions.get(transactionId);
        if (record == null) {
            record = new TransactionRecord(transactionId);
        }
        return record;
    }
	
	public void putTransactionRecord(TransactionRecord record) {
        transactions.put(record.getTransactionId(), record);
    }
    
    public void weakPutTransactionRecord(TransactionRecord record) {
        transactions.put(record.getTransactionId(), record);
    }
	
	public Record get(String table, String key) {
		check(table);
    	LOG.debug("get: " + table + " " + key);
        Record record = db.get(table).get(key);
        if (record == null) {
        	record = new Record(table, key);
        }
        return record;
    }
    
    public void put(Record record) {
    	LOG.debug("put: " + record.getTable() + " " + record.getKey());
    	String table = record.getTable();
    	String key = record.getKey();
    	db.get(table).put(key, record);
    	
		// maintain secondary index
		if (secondaryIndexInfo.containsKey(table)) {
			List<String> indexList = secondaryIndexInfo.get(table);
			String indexKey = DTCCUtil.buildKey(indexList);
			Set<String> primaryKeyList = secondaryIndex.get(table).get(indexKey);
			if (primaryKeyList == null) {
				primaryKeyList = new ConcurrentSkipListSet<String>();
				secondaryIndex.get(table).put(indexKey, primaryKeyList);
			}
			primaryKeyList.add(key);
		}
    }
    
    public void weakPut(Record record) {
    	put(record);
    }
    
    // secondary index
    public boolean createSecondaryIndex(String table, List<String> fields) {
    	LOG.debug("createSecondaryIndex: " + table);
		LOG.debug("create secondaryIndex Table: " + table + " Fields: " + fields);
		secondaryIndexInfo.put(table, fields);
		secondaryIndex.put(table, new ConcurrentHashMap<String, Set<String>>());
		return true;
	}
    
    public synchronized boolean write(String table, String key, List<String> names, List<String> values) {
//    	LOG.debug("write: " + table + " " + key);
    	if (!db.containsKey(table)) {db.put(table, new ConcurrentHashMap<String, Record>()); }
		int size = names.size();
		ConcurrentHashMap<String, Record> m = db.get(table);
		Record r = m.get(key);
		if (r == null) {
			r = new Record(table, key, 1);
			m.put(key, r);
		}
		for (int i = 0; i < size; i++) { r.put(names.get(i), values.get(i)); }
		
		// maintain secondary index
		if (secondaryIndexInfo.containsKey(table)) {
			List<String> indexList = secondaryIndexInfo.get(table);
			String indexKey = DTCCUtil.buildKey(indexList);
			Set<String> primaryKeyList = secondaryIndex.get(table).get(indexKey);
			if (primaryKeyList == null) {
				primaryKeyList = new ConcurrentSkipListSet<String>();
				secondaryIndex.get(table).put(indexKey, primaryKeyList);
			}
			primaryKeyList.add(key);
		}
		return true;
	}
    
    // read from secondary index
    public List<Map<String, String>> read_secondaryIndex(String table, String key,
			List<String> names, boolean isAll) {
		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
		if (!isAll) {
			Iterator<String> i = secondaryIndex.get(table).get(key).iterator();
		
			String primaryKey = i.next();
			list.add(read(table, primaryKey, names));
		} else {
			for(String primaryKey : secondaryIndex.get(table).get(key)) {
				list.add(read(table, primaryKey, names));
			}
		}
		return list;
	}
    
    /* Basic read */
	public HashMap<String, String> read(String table, String key,
			List<String> names) {
    	LOG.debug("read: " + table + " " + key);
		Record record = db.get(table).get(key);
		if (record != null) {
			HashMap<String, String> map = new HashMap<String, String>();
			for (String name : names) {
				if (record.getValue(name) != null) {
					map.put(name, record.getValue(name));
				}
			}
			return map;
		}
		return null;
	}
	
	public boolean add(String table, String key, List<String> names, List<String> values, boolean isDecimal) {
		LOG.debug(DTCCUtil.buildString("ADD: ", table, " ", key));
		check(table);
		if (names == null || values == null || values.size() != names.size()) {
			return false;
		}
		int size = names.size();
		Record r = db.get(table).get(key);
		if (r == null) {
			throw new DTCCException("No such element table:" + table + " key: "+ key);
		}
		for (int i = 0; i < size; i++) {
//			LOG.debug("Table: " + table + " Key: " + key + " Add: " + names.get(i) + values.get(i));
			String new_value;
			if (isDecimal) {
				new_value = String.valueOf(Float.parseFloat(r.getValue(names.get(i))) + Float.parseFloat(values.get(i)));
			} else {
				new_value = String.valueOf(Integer.parseInt(r.getValue(names.get(i))) + Integer.parseInt(values.get(i)));
			}
			r.put(names.get(i), new_value);
		}
		return true;
	}

	
	public boolean delete(String table, String key) {
		LOG.debug("Delete table: " + table + " key: " + key);
		Record r = db.get(table).get(key);
		if (secondaryIndexInfo.containsKey(table)) {
			StringBuffer stringBuffer = new StringBuffer();
			List<String> indexList =  secondaryIndexInfo.get(table);
			for(int i = 0; i < indexList.size() - 1; i++) {
				stringBuffer.append(r.getValue(indexList.get(i)));
				stringBuffer.append("_");
			}
			stringBuffer.append(r.getValue(indexList.get(indexList.size() - 1)));
			String indexKey = stringBuffer.toString();
			secondaryIndex.get(table).get(indexKey).remove(key);
		}
		db.get(table).remove(key);
		return true;
	}
}
