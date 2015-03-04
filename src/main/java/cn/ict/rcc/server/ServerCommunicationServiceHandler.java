package cn.ict.rcc.server;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.ReturnType;
import cn.ict.rcc.messaging.RococoCommunicationService.Iface;

/**
 * Server Service Handler
 * @author Wei Hu
 */
public class ServerCommunicationServiceHandler implements Iface {

	private static final Log LOG = LogFactory.getLog(ServerCommunicationServiceHandler.class);
			
	private AgentService agent;
	
	public ServerCommunicationServiceHandler(AgentService agent) {
        this.agent = agent;
    }
	
	@Override
	public boolean ping() throws TException {
		LOG.info("Server Handler: received ping");
		return true;
	}

	@Override
	public ReturnType start_req(Piece piece) throws TException {
		LOG.info("Server Handler: start_req(Piece piece)");
		return agent.start_req(piece);
	}

	@Override
	public ReturnType commit_req(String transactionId, Piece piece) throws TException {
		LOG.info("Server Handler: commit_req");
		return agent.commit_req(transactionId, piece);
	}

	@Override
	public boolean write(String table, String key, List<String> names,
			List<String> values) throws TException {
		return agent.write(table, key, names, values);
	}

}
