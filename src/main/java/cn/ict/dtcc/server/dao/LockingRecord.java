package cn.ict.dtcc.server.dao;

import java.util.TreeSet;

public class LockingRecord {

	public String tid;

	public TreeSet<String> keyList;

	public LockingRecord(String tid, String[] keys) {
		this.tid = tid;
		keyList = new TreeSet<String>();
		for (String key : keys) {
			keyList.add(key);
		}
	}

	/**
	 * @return true if all keys are in the list already; false if any one of the
	 *         keys is not in the list
	 */
	public boolean addLocks(String[] keys) {
		boolean result = true;
		for (String key : keys) {
			result &= !this.keyList.add(key);
		}
		return result;
	}

}
