package cn.ict.dtcc.server.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

public class LockingMemoryDB extends MemoryDB {

	private HashMap<String, ArrayList<LockingRecord>> tableLockList;
	private HashMap<String, ArrayList<String>> tableLocktids;

	public LockingMemoryDB() {
		super();
		this.tableLockList = new HashMap<String, ArrayList<LockingRecord>>();
		this.tableLocktids = new HashMap<String, ArrayList<String>>();
	}

	public synchronized void locksAppend(String tid, String table, String[] keys) {

		if (keys.length == 0)
			return;

		ArrayList<LockingRecord> rowLockList = tableLockList.get(table);
		ArrayList<String> tidList;
		if (rowLockList == null) {
			rowLockList = new ArrayList<LockingRecord>();
			tableLockList.put(table, rowLockList);
			tidList = new ArrayList<String>();
			tableLocktids.put(table, tidList);
		} else {
			tidList = tableLocktids.get(table);
		}

		int pos = tidList.indexOf(tid);
		if (pos == -1) { // this transaction never locks any keys on that table
			rowLockList.add(new LockingRecord(tid, keys));
			tidList.add(tid);
		} else {
			if (!rowLockList.get(pos).addLocks(keys)) {
				LockingRecord trec = rowLockList.get(pos);
				// reinsert the locking list
				rowLockList.remove(pos);
				rowLockList.add(trec);
				tidList.remove(pos);
				tidList.add(tid);
			}
		}
	}

	public synchronized void locksRemove(String table, String tid) {
		ArrayList<String> tidList = tableLocktids.get(table);
		if (tidList != null) {
			int pos = tidList.indexOf(tid);
			if (pos != -1) {
				tidList.remove(tid);
				tableLockList.get(table).remove(pos);
			}
		}
	}

	public boolean isNonConflictingHead(String table, String tid) {

		ArrayList<String> tidList = tableLocktids.get(table);
		if (tidList != null) {
			int pos = tidList.indexOf(tid);
			if (pos == -1) {
				return true; // we are not locking any data item; definitely nonConflictingHead
			}

			// long countTime = System.nanoTime();
			ArrayList<LockingRecord> rowLockList = tableLockList.get(table);
			TreeSet<String> toCompare = rowLockList.get(pos).keyList;
			for (int i = 0; i < pos; i++) {
				for(String key : rowLockList.get(i).keyList) {
					if (toCompare.contains(key)) {
						return false; // intersection
					}
				}
			}
			// System.err.println(System.nanoTime()-countTime);
			return true; //no intersection
		}
		return true;
	}
}
