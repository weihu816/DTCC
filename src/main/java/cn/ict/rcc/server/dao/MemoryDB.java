package cn.ict.rcc.server.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.rcc.benchmark.tpcc.TPCCLoader;
import cn.ict.rcc.exception.RococoException;

/**
 * A implementation of Memory Database
 * 
 * @author Wei Hu
 */

public class MemoryDB {

	private static final Log LOG = LogFactory.getLog(MemoryDB.class);

	private Map<String, ConcurrentHashMap<String, Record>> db = new ConcurrentHashMap<String, ConcurrentHashMap<String, Record>>();

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
			throw new RococoException("No such element");
		}
		for (int i = 0; i < size; i++) {
			String new_value = String.valueOf(Integer.parseInt(r.get(names.get(i))) + Integer.parseInt(values.get(i)));
			r.put(names.get(i), new_value);
LOG.info("Table: " + table + " Key: " + key + " Add: " + names.get(i) + values.get(i));
		}
		return true;
	}

}
