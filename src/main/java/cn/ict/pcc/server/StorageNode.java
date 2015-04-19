package cn.ict.pcc.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import cn.ict.dtcc.config.ServerConfiguration;
import cn.ict.dtcc.server.dao.LockingMemoryDB;
import cn.ict.dtcc.server.dao.Record;
import cn.ict.dtcc.server.dao.TransactionRecord;
import cn.ict.occ.appserver.Accept;
import cn.ict.occ.appserver.Option;
import cn.ict.occ.messaging.ReadValue;
import cn.ict.occ.server.OCCCommunicator;


public class StorageNode {

	private static final Log LOG = LogFactory.getLog(StorageNode.class);
	
	private LockingMemoryDB db = new LockingMemoryDB();
	private ServerConfiguration config;
    private OCCCommunicator communicator;
    
    private Random rand = new Random(System.nanoTime());
	private void randomBackoff() {
		try {
			long sleepTime = (long) (rand.nextInt(10000) % 300);
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
    public StorageNode() {
        this.config = ServerConfiguration.getConfiguration();
        this.communicator = new OCCCommunicator();
    }
    
    public void start() {
        db.init();
        int port = config.getLocalMember().getPort();
// TODO       communicator.startListener(this, port);
    }
    
    public void stop() {
        db.shutdown();
        communicator.stopListener();
        communicator.stopSender();
    }
    
	public ReadValue onRead(String tid, String table, String key, List<String> names) {
		this.db.locksAppend(tid, table, new String[] { key });
		while (true) {
			if (this.db.isNonConflictingHead(table, tid)) {
				LOG.info("Lock Success! ====" + tid + " table: " + table);
				break;
			} else {
				LOG.info("Sleeping~ txn:" + tid + " table:" + table + " key:" + key);
				randomBackoff();
			}
		}
		Record record = db.get(table, key);
		List<String> values = new ArrayList<String>();
		if (record.getVersion() > 0) {
			for (String name : names) {
				values.add(record.getValue(name));
			}
		}
		LOG.debug("Return " + values);
		return new ReadValue(record.getVersion(), values);
	}
    
    public boolean onAccept(Accept accept) {
    	
        if (LOG.isDebugEnabled()) { LOG.debug("Received accept message: " + accept); }

        String table 		= accept.getTable();
        String key   		= accept.getKey();
        String transaction 	= accept.getTransactionId();
        long oldVersion 	= accept.getOldVersion();
        List<String> names 	= accept.getNames();
        List<String> values = accept.getValues();

        synchronized (table.intern()) {
        	synchronized (key.intern()) {
        		Record record = db.get(table, key);
        		// if record has been written by another transaction
                if (record.getOutstanding() != null && !transaction.equals(record.getOutstanding())) {
                	LOG.warn("Outstanding option detected on " + table + " " +  key + " - Denying the new option (" + record.getOutstanding() + ")");
                    synchronized (transaction.intern()) {
                        TransactionRecord txnRecord = db.getTransactionRecord(transaction);
                        Option option = new Option(table, key, names, values, record.getVersion()); // dirty:false
                        switch (txnRecord.getStatus()) {
                            case TransactionRecord.STATUS_COMMITTED:
                                txnRecord.addOption(option);
                                if (record.getVersion() <= option.getOldVersion()) {
                                    record.setVersion(option.getOldVersion() + 1);
                                    Map<String, String> writeValues = new HashMap<String, String>();
    	                            for (int i = 0; i < option.getNames().size(); i++) {
    	                            	writeValues.put(option.getNames().get(i), option.getValues().get(i));
    	                            }
                                    record.setValues(writeValues);
                                    record.setOutstanding(null);
                                    db.put(record);
                                }
                                db.putTransactionRecord(txnRecord);
                                LOG.info("Applied delayed option");
                                break;
                            case TransactionRecord.STATUS_ABORTED:
                                break;
                            default:
                                txnRecord.addOption(option);
                                db.weakPutTransactionRecord(txnRecord);
                        }
                    }
                    return false;
                }

        		// succeed if the version of the record is equal to version of Accept
				long version = record.getVersion();
				boolean success = (version == oldVersion);

				if (success) {
					record.setOutstanding(transaction);
					db.weakPut(record);
					LOG.debug("option accepted");
				} else {
					LOG.debug("option denied");
				}				
				
	            synchronized (transaction.intern()) {
	                TransactionRecord txnRecord = db.getTransactionRecord(transaction);
	                Option option = new Option(table, key, names, values, record.getVersion()); // dirty:false
	                switch (txnRecord.getStatus()) {
	                    case TransactionRecord.STATUS_COMMITTED:
	                        txnRecord.addOption(option);
	                        if (record.getVersion() <= option.getOldVersion()) {
	                            record.setVersion(option.getOldVersion() + 1);
	                            Map<String, String> writeValues = new HashMap<String, String>();
	                            for (int i = 0; i < option.getNames().size(); i++) {
	                            	writeValues.put(option.getNames().get(i), option.getValues().get(i));
	                            }
	                            record.setValues(writeValues);
	                            record.setOutstanding(null);//TODO
	                            db.put(record);
	                        }
	                        db.putTransactionRecord(txnRecord);
	                        LOG.info("Applied delayed option");
	                        break;
	                    case TransactionRecord.STATUS_ABORTED:
	                        break;
	                    default:
	                        txnRecord.addOption(option);
	                        db.weakPutTransactionRecord(txnRecord);
	                }
	            }
	            return success;
        	}
        }
    }
    
    public void onDecide(String transaction, boolean commit) {
        synchronized (transaction.intern()) {
            TransactionRecord txnRecord = db.getTransactionRecord(transaction);
            if (commit) {
                LOG.debug("Received Commit decision on transaction id: " + transaction);
                for (Option option : txnRecord.getOptions()) {
                    synchronized (option.getTable().intern()) {
						synchronized (option.getKey().intern()) {
							Record record = db.get(option.getTable(), option.getKey());
	                        if (record.getVersion() <= option.getOldVersion()) {
	                            record.setVersion(option.getOldVersion() + 1);
	                            Map<String, String> writeValues = new HashMap<String, String>();
	                            for (int i = 0; i < option.getNames().size(); i++) {
	                            	writeValues.put(option.getNames().get(i), option.getValues().get(i));
	                            }
	                            record.setValues(writeValues);
	                            record.setOutstanding(null);
	                            db.put(record);
	                            
	                        }
						}
                    }
                    LOG.debug("[COMMIT] Saved option to DB");
                }
            } else {
            	LOG.info("Received Abort on transaction id: " + transaction);
                for (Option option : txnRecord.getOptions()) {
                	
					synchronized (option.getTable().intern()) {
						synchronized (option.getKey().intern()) {
							Record record = db.get(option.getTable(), option.getKey());
							if (transaction.equals(record.getOutstanding())) {
								record.setOutstanding(null);
							}
							db.put(record);
						}
					}
					
                    LOG.debug("[ABORT] Not saving option to DB");
                }
            }

            if (txnRecord.getStatus() == TransactionRecord.STATUS_UNDECIDED) {
                txnRecord.finish(commit);
                db.putTransactionRecord(txnRecord);
            }
        }
    }
    
    
    public synchronized boolean write(String table, String key,
			List<String> names, List<String> values) {
		return db.write(table, key, names, values);
	}
	
	public boolean createSecondaryIndex(String table, List<String> fields) {
		return db.createSecondaryIndex(table, fields);
	}
	
    public static void main(String[] args) {
    	PropertyConfigurator.configure(ServerConfiguration.getConfiguration().getLogConfigFilePath());
        final StorageNode storageNode = new StorageNode();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                storageNode.stop();
            }
        });
        storageNode.start();
	}
}
