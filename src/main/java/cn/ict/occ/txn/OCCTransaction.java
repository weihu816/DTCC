package cn.ict.occ.txn;

import java.util.Collection;
import java.util.List;

import cn.ict.dtcc.exception.TransactionException;
import cn.ict.occ.appserver.Result;
import cn.ict.occ.appserver.Transaction;
import cn.ict.occ.messaging.AppServerService;
import cn.ict.occ.server.dao.Option;


public class OCCTransaction extends Transaction {
	private AppServerService appServer;

	public OCCTransaction(AppServerService appServer) {
        super();
        this.appServer = appServer;
    }
	
	@Override
	protected Result doRead(String table, String key, List<String> names) {
        return appServer.read(table, key, names);
	}

    @Override
    protected void doCommit(String transactionId,
                            Collection<Option> options) throws TransactionException {
//        boolean success = appServer.commit(transactionId, options);
//        if (!success) {
//            throw new TransactionException("Failed to commit txn: " + transactionId);
//        }
    }

	
}
