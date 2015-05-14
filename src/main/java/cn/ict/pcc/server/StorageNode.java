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
import cn.ict.pcc.messaging.Accept;
import cn.ict.pcc.messaging.Option;
import cn.ict.pcc.messaging.ReadValue;


public class StorageNode {

	private static final Log LOG = LogFactory.getLog(StorageNode.class);
	
	private LockingMemoryDB db = new LockingMemoryDB();
	private ServerConfiguration config;
    private PCCCommunicator communicator;
    
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
        this.communicator = new PCCCommunicator();
    }
    
    public void start() {
        db.init();
        int port = config.getLocalMember().getPort();
        communicator.startListener(this, port);
    }
    
    public void stop() {
        db.shutdown();
        communicator.stopListener();
        communicator.stopSender();
    }
    
	public ReadValue onRead(String tid, String table, String key, List<String> names) {
		LOG.debug("onRead: table=" + table + " key=" + key + " " + names);
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
		return new ReadValue(values);
	}
    
    public boolean onAccept(Accept accept) {
    	return false;
    }
    
    public void onDecide(String transaction, boolean commit) {
       
    }
    

    /*
     * Plain write
     */
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
