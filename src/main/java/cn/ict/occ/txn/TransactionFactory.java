package cn.ict.occ.txn;

import cn.ict.occ.appserver.AppServer;
import cn.ict.occ.messaging.AppServerService;

public class TransactionFactory {
	
	private boolean local;
	private AppServerService appServer;

	public TransactionFactory() {
		this.local = true;
		this.appServer = new AppServer();
	}

	// HUWEI
	public OCCTransaction create() {
		return new OCCTransaction(appServer);
	}

	public void close() {
		appServer.stop();
	}

	public boolean isLocal() {
		return local;
	}
}
