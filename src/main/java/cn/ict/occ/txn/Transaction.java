package cn.ict.occ.txn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.dtcc.benchmark.tpcc.TPCCGenerator;
import cn.ict.dtcc.exception.TransactionException;
import cn.ict.dtcc.server.dao.Database;
import cn.ict.occ.appserver.Option;
import cn.ict.occ.messaging.Result;

public abstract class Transaction {

	private static final Log LOG = LogFactory.getLog(Transaction.class);
	
    protected String transactionId;
    protected boolean complete;

    private Map<String, Map<String, Result>> readSet = new HashMap<String, Map<String, Result>>();
    protected Map<String, Map<String, Option>> writeSet = new HashMap<String, Map<String, Option>>();

    public void begin() {
        this.transactionId = UUID.randomUUID().toString();
    }

    public synchronized List<String> read(String table, String key, List<String> names) throws TransactionException {
        assertState();
        boolean toRead = false;
        List<String> toReturn = new ArrayList<String>();

        if (readSet.containsKey(table) && readSet.get(table).containsKey(key)) {
            Result result = readSet.get(table).get(key);
            Map<String, String> values = result.getValues();
            for (String name : names) {
            	if (!values.containsKey(name)) {
            		toRead = true;
            		break;
            	}
            	toReturn.add(values.get(name));
            }
            if (!toRead){
            	if (result.isDeleted()) {
                	throw new TransactionException("No object exists by : " + table + " " + key);
                }
                if (result.getVersion() == 0) {
                	throw new TransactionException("No object exists by : " + table + " " + key);
                }
                return toReturn;
            }
        } else { toRead = true; }
        
        if (toRead) {
            Result result = doRead(table, key, names);
            if (result != null) {
//                result.setDeleted(isDeleted(result.getValues().get(Database.DELETE_FIELD)));
                
                if (!readSet.containsKey(table)) { readSet.put(table, new HashMap<String, Result>()); }
                Map<String, Result> x  = readSet.get(table);
                x.put(key, result);

                if (result.isDeleted()) {
                	throw new TransactionException("No object exists by : " + table + " " + key);
                }

                if (result.getVersion() == 0) {
                	throw new TransactionException("No object exists by : " + table + " " + key);
                }
                Map<String, String> values = result.getValues();
                for (String name : names) {
                	toReturn.add(values.get(name));
                }
            } else {
                throw new TransactionException("No object exists by : " + table + " " + key);
            }
        }
        return toReturn;
    }

	public synchronized List<String> readIndexFetchMiddle(String table,
			String key, List<String> names, String orderField,
			Boolean isAssending) throws TransactionException {
        assertState();
        List<String> toReturn = new ArrayList<String>();

        Result result = doreadIndexFetchMiddle(table, key, names, orderField, isAssending);
        if (result != null) {
//        	result.setDeleted(isDeleted(result.getValues().get(Database.DELETE_FIELD)));
        	if (!readSet.containsKey(table)) { readSet.put(table, new HashMap<String, Result>()); }
        	Map<String, Result> x = readSet.get(table);
        	x.put(result.getKey(), result); // TODO

        	if (result.isDeleted()) { throw new TransactionException("No object exists by : " + table + " " + key); }

        	if (result.getVersion() == 0) {
        		throw new TransactionException("No object exists by : " + table + " " + key);
        	}
        	Map<String, String> values = result.getValues();
        	for (String name : names) {
        		toReturn.add(values.get(name));
        	}
        	if (!readSet.containsKey(table)) { readSet.put(table, new HashMap<String, Result>()); }
            readSet.get(table).put(result.getKey(), result);
        } else {
        	throw new TransactionException("No object exists by : " + table + " " + key);
        }
        
        return toReturn;
    }
	
	public synchronized List<List<String>> readIndexFetchAll(String table,
			String key, List<String> names) throws TransactionException {
        assertState();
        List<List<String>> toReturn = new ArrayList<List<String>>();

        List<Result> results = doreadIndexFetchAll(table, key, names);
        for (Result result : results) {
        	if (result != null) {
//        		result.setDeleted(isDeleted(result.getValues().get(Database.DELETE_FIELD)));
        		if (!readSet.containsKey(table)) { readSet.put(table, new HashMap<String, Result>()); }
        		Map<String, Result> x = readSet.get(table);
        		x.put(result.getKey(), result); // TODO

        		if (result.isDeleted()) { throw new TransactionException("No object exists by : " + table + " " + key); }

        		if (result.getVersion() == 0) {
        			throw new TransactionException("No object exists by : " + table + " " + key);
        		}
        		Map<String, String> values = result.getValues();
            	List<String> list = new ArrayList<String>();
        		for (String name : names) {
        			list.add(values.get(name));
        		}
        		toReturn.add(list);
        		if (!readSet.containsKey(table)) { readSet.put(table, new HashMap<String, Result>()); }
                readSet.get(table).put(result.getKey(), result);
        	} else {
        		throw new TransactionException("No object exists by : " + table + " " + key);
        	}
        }
        return toReturn;
    }
    
	public synchronized List<String> readIndexFetchTop(String table, String key, 
			List<String> names, String orderField, boolean isAssending) throws TransactionException {
		
        assertState();
        List<String> toReturn = new ArrayList<String>();
        
        Result result = doreadIndexFetchTop(table, key, names, orderField, isAssending);
        if (result != null) {
//        	result.setDeleted(isDeleted(result.getValues().get(Database.DELETE_FIELD)));
        	if (!readSet.containsKey(table)) { readSet.put(table, new HashMap<String, Result>()); }
        	Map<String, Result> x  = readSet.get(table);
        	x.put(key, result);

        	if (result.isDeleted()) {
        		throw new TransactionException("No object exists by readIndexFetchTop : " + table + " " + key);
        	}

        	if (result.getVersion() == 0) {
        		throw new TransactionException("No object exists by readIndexFetchTop: " + table + " " + key);
        	}
        	Map<String, String> values = result.getValues();
        	for (String name : names) {
        		toReturn.add(values.get(name));
        	}
        	if (!readSet.containsKey(table)) { readSet.put(table, new HashMap<String, Result>()); }
            readSet.get(table).put(result.getKey(), result);
        } else {
        	throw new TransactionException("No object exists by readIndexFetchTop: " + table + " " + key);
        }
        return toReturn;
    }

    public synchronized void delete(String table, String key) throws TransactionException {
        assertState();

        Option option;
        Result result = readSet.get(table).get(key);
        if (result != null) {
            // We have already read this object.
            // Update the value in the read-set so future reads can see this write.
            if (result.isDeleted()) {
                throw new TransactionException("Object already deleted: " + key);
            }
            option = new Option(table, key, Database.DELETE_NAME, Database.DELETE_VALUE, result.getVersion());
        } else {
            result = doRead(table, key, new ArrayList<String>());
            if (result == null) {
                // Object doesn't exist in the DB - Error!
                throw new TransactionException("Unable to delete non existing object: " + key);
            } else {
                // Object exists in the DB.
                // Update the value and add to the read-set so future reads can see this write.
                result.setDeleted(true);
                option = new Option(table, key, Database.DELETE_NAME, Database.DELETE_VALUE, result.getVersion());
                if (!readSet.containsKey(table)) { readSet.put(table, new HashMap<String, Result>()); }
                readSet.get(table).put(result.getKey(), result);
            }
        }
        if (!writeSet.containsKey(table)) { writeSet.put(table, new HashMap<String, Option>()); }
        writeSet.get(table).put(key, option);
    }

    public synchronized void write(String table, String key, List<String> names, List<String> values) throws TransactionException {
        assertState();
        
        boolean toRead = true;
        Map<String, String> writeValues = new HashMap<String, String>();
        for (int i = 0; i < names.size(); i++) {
        	writeValues.put(names.get(i), values.get(i));
        }

        Option option = null;
        if (readSet.containsKey(table) && readSet.get(table).containsKey(key)) {
        	toRead = false;
            Result result = readSet.get(table).get(key);
            Map<String, String> readValues = result.getValues();
            for (String name : names) {
            	if (!readValues.containsKey(name)) {
            		toRead = true;
            		break;
            	}
            }
            if (!toRead) {
            	// We have already read this object.
                // Update the value in the read-set so future reads can see this write.
                option = new Option(table, key, names, values, result.getVersion());
                result.getValues().putAll(writeValues);
            }
        } 
        if (toRead) {
            // We haven't read this object before (blind write).
            // Do an implicit read from the database.
            Result result = doRead(table, key, names);
            if (result == null) {
                // Object doesn't exist in the DB - Insert (version = 0)
                result = new Result(table, key, writeValues, (long) 0);
            } else {
                // Object exists in the DB.
                // Update the value and add to the read-set so future reads can
                // see this write.
                result.getValues().putAll(writeValues);
            }
            option = new Option(table, key, names, values, result.getVersion());
            if (!readSet.containsKey(table)) { readSet.put(table, new HashMap<String, Result>()); }
            readSet.get(table).put(key, result);
        }
        if (!writeSet.containsKey(table)) { writeSet.put(table, new HashMap<String, Option>()); }
        writeSet.get(table).put(key, option);
    }

    public synchronized void commit() throws TransactionException {
        assertState();
        try {
            if (writeSet.size() > 0) {
            	List<Option> options = new ArrayList<Option>();
            	for (Map<String, Option> m : writeSet.values()) {
            		options.addAll(m.values());
            	}
                doCommit(transactionId, options);
            }
        } finally {
            this.complete = true;
        }
    }

    private void assertState() throws TransactionException {
        if (this.transactionId == null) {
            throw new TransactionException("Read operation invoked before begin");
        } else if (this.complete) {
            throw new TransactionException("Attempted operation on completed transaction");
        }
    }

    protected abstract Result doRead(String table, String key, List<String> names);
    
    protected abstract Result doreadIndexFetchMiddle(String table, String keyIndex, List<String> names, String orderField, boolean isAssending);

    protected abstract Result doreadIndexFetchTop(String table, String keyIndex, List<String> names, String orderField, boolean isAssending);

    protected abstract List<Result> doreadIndexFetchAll(String table, String keyIndex, List<String> names);

    protected abstract void doCommit(String transactionId, Collection<Option> options) throws TransactionException;

}
