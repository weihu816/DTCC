package cn.ict.pcc.txn;

import java.util.Collection;
import java.util.List;

import cn.ict.dtcc.exception.TransactionException;
import cn.ict.pcc.appserver.AppServer;
import cn.ict.pcc.appserver.Option;
import cn.ict.pcc.messaging.Result;


public class PCCTransaction extends Transaction {
	
	private AppServer appServer;

	public PCCTransaction(AppServer appServer) {
        super();
        this.appServer = appServer;
    }
	
	@Override
	protected Result doRead(String txnid, String table, String key, List<String> names) {
        return appServer.read(txnid, table, key, names);
	}

	@Override
	protected void doCommit(String transactionId, Collection<Option> options)
			throws TransactionException {
		boolean success = appServer.commit(transactionId, options);
        if (!success) {
            throw new TransactionException("Failed to commit txn: " + transactionId);
        }
	}

}
