package cn.ict.rcc.server.coordinator.messaging;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

	public static volatile CoordinatorCommunicator communicator = null;
	
	private static final Log LOG = LogFactory.getLog(CoordinatorCommunicator.class);
	private ServerConfiguration config;

	private KeyedObjectPool<Member, TTransport> blockingPool = new StackKeyedObjectPool<Member, TTransport>(
			new ThriftConnectionPool());
	private KeyedObjectPool<Member, TNonblockingSocket> nonBlockingPool = new StackKeyedObjectPool<Member, TNonblockingSocket>(
			new ThriftNonBlockingConnectionPool());
	private HashMap<Member, RococoCommunicationService.AsyncClient> asyncClientPool = new HashMap<Member, RococoCommunicationService.AsyncClient>();
	private HashMap<Member, RococoCommunicationService.Client> clientPool = new HashMap<Member, RococoCommunicationService.Client>();

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
		LOG.debug("fistRound Txn:" + piece.getTransactionId() + " " + piece.getTable() + " " + piece.getKey());
		MethodCallback callback = new MethodCallback();
		Member member = null;
//		ReturnType returnType = null;
		TNonblockingTransport transport = null;
		try {
			member = config.getShardMember(piece.getTable(), piece.getKey());
			LOG.debug("fistRound(Piece piece) " + member.getHostName() + " " + member.getPort());
			RococoCommunicationService.AsyncClient asyncClient = asyncClientPool.get(member);
			if (asyncClient == null) {
				transport = nonBlockingPool.borrowObject(member);
				TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
				asyncClient = new RococoCommunicationService.AsyncClient(protocolFactory, clientManager, transport);
				asyncClientPool.put(member, asyncClient);
			}
			asyncClient.start_req(piece, callback);
//			returnType = callback.getResult();
		} catch (Exception e) {
			if (member != null) {
				handleException(member.getHostName(), e);
			}
		}
		return callback;
	}
	
	public boolean secondRound(String transactionId, List<Piece> pieces,
			Graph dep) throws TException {
		LOG.debug("secondRound Txn: " + transactionId + " " + dep);
		TTransport transport = null;
		Set<Member> members = new HashSet<Member>();
		for (Piece p : pieces) {
			members.add(config.getShardMember(p.getTable(), p.getKey()));
		}
		for (Member member : members) {
			RococoCommunicationService.Client client = clientPool.get(member);
			if (client == null) {
				try {
					transport = blockingPool.borrowObject(member);
				} catch (Exception e) {
					handleException(member.getHostName(), e);
				}
				client = new RococoCommunicationService.Client(new TBinaryProtocol(transport));
				clientPool.put(member, client);
			}
			client.commit_req(transactionId, dep);
		}

		return true;
	}

	private void handleException(String target, Exception e) {
		String msg = "Error contacting the remote member: " + target;
		LOG.warn(msg, e);
	}
	
	public static CoordinatorCommunicator getCoordinatorCommunicator() {
		if (communicator == null) {
			communicator = new CoordinatorCommunicator();
		}
		return communicator;
	}
}
