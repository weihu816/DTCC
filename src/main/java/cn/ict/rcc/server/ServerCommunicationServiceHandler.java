package cn.ict.rcc.server;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import cn.ict.rcc.messaging.Edge;
import cn.ict.rcc.messaging.Graph;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.ReturnType;
import cn.ict.rcc.messaging.RococoCommunicationService.Iface;

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
		LOG.info("Server Handler: received ping");
		return true;
	}

	@Override
	public ReturnType start_req(Piece piece) throws TException {
		LOG.info("Server Handler: start_req(Piece piece) TransactionID: " + piece.getTransactionId());
		return node.start_req(piece);
	}


	@Override
	public boolean write(String table, String key, List<String> names,
			List<String> values) throws TException {
		return node.write(table, key, names, values);
	}

	@Override
	public ReturnType commit_req(String transactionId, Graph dep) throws TException {
		return node.commit_req(transactionId, dep);
	}

}
