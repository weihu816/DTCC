package cn.ict.occ.txn;

import java.util.Collection;
import java.util.List;

import cn.ict.dtcc.exception.TransactionException;
import cn.ict.occ.appserver.AppServerService;
import cn.ict.occ.appserver.Option;
import cn.ict.occ.messaging.Result;


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
        boolean success = appServer.commit(transactionId, options);
        if (!success) {
            throw new TransactionException("Failed to commit txn: " + transactionId);
        }
    }

	
}
