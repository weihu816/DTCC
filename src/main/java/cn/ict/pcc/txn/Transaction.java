package cn.ict.pcc.txn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.dtcc.exception.TransactionException;
import cn.ict.pcc.appserver.Option;
import cn.ict.pcc.messaging.Result;

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
                return toReturn;
            }
        } else { toRead = true; }
        
        if (toRead) {
            Result result = doRead(transactionId, table, key, names);
            if (result != null) {                
                if (!readSet.containsKey(table)) { readSet.put(table, new HashMap<String, Result>()); }
                Map<String, Result> x  = readSet.get(table);
                x.put(key, result);
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
                option = new Option(table, key, names, values);
                result.getValues().putAll(writeValues);
            }
        } 
        if (toRead) {
            Result result = doRead(transactionId, table, key, names);
            if (result == null) {
                result = new Result(table, key, writeValues);
            } else {
                result.getValues().putAll(writeValues);
            }
            option = new Option(table, key, names, values);
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

    protected abstract Result doRead(String transactionId, String table, String key, List<String> names);

    protected abstract void doCommit(String transactionId, Collection<Option> options) throws TransactionException;

}
