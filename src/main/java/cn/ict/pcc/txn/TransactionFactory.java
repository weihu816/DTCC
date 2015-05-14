package cn.ict.pcc.txn;

import cn.ict.pcc.appserver.AppServer;


public class TransactionFactory {
	
	private boolean local;
	private AppServer appServer;

	public TransactionFactory() {
		this.local = true;
		this.appServer = new AppServer();
	}

	// HUWEI
	public PCCTransaction create() {
		return new PCCTransaction(appServer);
	}

	public void close() {
		appServer.stop();
	}

	public boolean isLocal() {
		return local;
	}
}
