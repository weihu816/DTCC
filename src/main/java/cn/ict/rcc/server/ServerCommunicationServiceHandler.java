package cn.ict.rcc.server;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import cn.ict.rcc.messaging.CommitResponse;
import cn.ict.rcc.messaging.Graph;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.RococoCommunicationService.Iface;
import cn.ict.rcc.messaging.StartResponse;
import cn.ict.rcc.messaging.StartResponseBulk;

/**
 * Server Service Handler
 * @author Wei Hu
 */
public class ServerCommunicationServiceHandler implements Iface {

	private static final Log LOG = LogFactory.getLog(ServerCommunicationServiceHandler.class);
			
	private StorageNode node;
	
	public ServerCommunicationServiceHandler(StorageNode node) {
        this.node = node;
    }
	
	@Override
	public boolean ping() throws TException {
		LOG.debug("Server Handler: received ping");
		return true;
	}

	@Override
	public StartResponse start_req(Piece piece) throws TException {
		LOG.debug("Server Handler: start_req(Piece piece) TransactionID: " + piece.getTransactionId());
		return null;
	}
	
	@Override
	public StartResponseBulk start_req_bulk(List<Piece> pieces) throws TException {
		return node.start_req(pieces);
	}

	@Override
	public boolean write(String table, String key, List<String> names,
			List<String> values) throws TException {
		return node.write(table, key, names, values);
	}

	@Override
	public CommitResponse commit_req(String transactionId, Graph dep) throws TException {
		if (node.status.get(transactionId) == StorageNode.STARTED) {
			node.status.put(transactionId, StorageNode.COMMITTING);
		} else {
			LOG.warn("ERROR: DECIDED");
		}
		LOG.debug("*** commit_req txn " + transactionId);
		return node.commit_req(transactionId, dep);
	}
	
	@Override
	public boolean createSecondaryIndex(String table, List<String> fields)
			throws TException {
		return node.createSecondaryIndex(table, fields);
	}

	@Override
	public boolean rcc_ask_txnCommitting(String transactionId)
			throws TException {
		return node.rcc_ask_txnCommitting(transactionId);
	}

	
}
