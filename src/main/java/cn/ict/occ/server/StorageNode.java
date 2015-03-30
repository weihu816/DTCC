package cn.ict.occ.server;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import cn.ict.dtcc.config.ServerConfiguration;
import cn.ict.occ.messaging.ReadValue;
import cn.ict.occ.server.dao.MemoryDB;
import cn.ict.occ.server.dao.Record;



public class StorageNode {

//	private static final Log log = LogFactory.getLog(StorageNode.class);
	
	private MemoryDB db = new MemoryDB();
	private ServerConfiguration config;

    private OCCCommunicator communicator;
    
    public StorageNode() {
        this.config = ServerConfiguration.getConfiguration();
        this.communicator = new OCCCommunicator();
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
    
    public ReadValue onRead(String table, String key, List<String> names) {
        Record record = db.get(table, key);
        List<String> values = new ArrayList<String>();
        for (String name : names) {
        	values.add(record.getValue(name));
        }
        return new ReadValue(record.getVersion(), values);
    }
    
    public static void main(String[] args) {
    	PropertyConfigurator.configure(ServerConfiguration.getConfiguration()
				.getLogConfigFilePath());
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
