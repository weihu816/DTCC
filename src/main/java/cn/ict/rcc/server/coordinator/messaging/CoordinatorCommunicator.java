package cn.ict.rcc.server.coordinator.messaging;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.StackKeyedObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;

import cn.ict.rcc.Member;
import cn.ict.rcc.exception.RococoException;
import cn.ict.rcc.messaging.Graph;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.RococoCommunicationService;
import cn.ict.rcc.messaging.ThriftConnectionPool;
import cn.ict.rcc.messaging.ThriftNonBlockingConnectionPool;
import cn.ict.rcc.server.config.ServerConfiguration;

/**
 * @author Wei
 */

public class CoordinatorCommunicator {

	private static final Log LOG = LogFactory.getLog(CoordinatorCommunicator.class);
	private ServerConfiguration config;

	private KeyedObjectPool<Member, TTransport> blockingPool = new StackKeyedObjectPool<Member, TTransport>(
			new ThriftConnectionPool());
	private KeyedObjectPool<Member, TNonblockingSocket> nonBlockingPool = new StackKeyedObjectPool<Member, TNonblockingSocket>(
			new ThriftNonBlockingConnectionPool());
	// TODO [wei] Client Pool

	private TAsyncClientManager clientManager;

	public CoordinatorCommunicator() {
		config = ServerConfiguration.getConfiguration();
		try {
			this.clientManager = new TAsyncClientManager();
		} catch (IOException e) {
			throw new RococoException("Failed to initialize Thrift client manager");
		}
	}

	public MethodCallback fistRound(Piece piece) throws TException {
		LOG.info("fistRound(Piece piece) " + piece.getTable() + " " + piece.getKey());
		MethodCallback callback = new MethodCallback();
		Member member = null;
//		ReturnType returnType = null;
		TNonblockingTransport transport = null;
		try {
			member = config.getShardMember(piece.getTable(), piece.getKey());
			transport = nonBlockingPool.borrowObject(member);
			TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
			RococoCommunicationService.AsyncClient AsyncClient = new
					RococoCommunicationService.AsyncClient(protocolFactory, clientManager, transport);
			AsyncClient.start_req(piece, callback);
//			returnType = callback.getResult();
		} catch (Exception e) {
			if (member != null) {
				handleException(member.getHostName(), e);
			}
		}
		return callback;
	}
	
	public boolean secondRound(String transactionId, List<Piece> pieces, Graph dep) throws TException {
		LOG.info("secondRound  " + dep);
		Member member = null;
		TTransport transport = null;
		try {
			 for (Piece p : pieces) {
				member = config.getShardMember(p.getTable(), p.getKey());
				transport = blockingPool.borrowObject(member);
				RococoCommunicationService.Client client = new RococoCommunicationService.Client(new TBinaryProtocol(transport));
				client.commit_req(transactionId, dep);
			 }
		} catch (Exception e) {
			if (member != null) {
				handleException(member.getHostName(), e);
			}
		}
		return true;
	}

	private void handleException(String target, Exception e) {
		String msg = "Error contacting the remote member: " + target;
		LOG.warn(msg, e);
	}
}
