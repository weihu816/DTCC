package cn.ict.rcc.server.coordinator.messaging;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.StackKeyedObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransport;

import cn.ict.rcc.Member;
import cn.ict.rcc.exception.RococoException;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.ReturnType;
import cn.ict.rcc.messaging.RococoCommunicationService;
import cn.ict.rcc.messaging.ThriftConnectionPool;
import cn.ict.rcc.messaging.ThriftNonBlockingConnectionPool;
import cn.ict.rcc.server.config.TpccServerConfiguration;

/**
 * TODO
 * @author Wei
 *
 */
public class CoordinatorCommunicator {

	private static final Log LOG = LogFactory.getLog(CoordinatorCommunicator.class);
	private TpccServerConfiguration config;

	private KeyedObjectPool<Member, TTransport> blockingPool = new StackKeyedObjectPool<Member, TTransport>(
			new ThriftConnectionPool());
	private KeyedObjectPool<Member, TNonblockingSocket> nonBlockingPool = new StackKeyedObjectPool<Member, TNonblockingSocket>(
			new ThriftNonBlockingConnectionPool());
	// TODO: Client Pool

	private TAsyncClientManager clientManager;

	public CoordinatorCommunicator() {
		config = TpccServerConfiguration.getConfiguration();
		try {
			this.clientManager = new TAsyncClientManager();
		} catch (IOException e) {
			throw new RococoException("Failed to initialize Thrift client manager");
		}
	}

	/**
	 * Send the immediate piece immediately 
	 */
	public ReturnType fistRound(Piece piece) throws TException {
		LOG.info("fistRoundAsync(Piece piece)");
		Member member = null;
		TTransport transport = null;
		ReturnType returnType = null;
		try {
			member = config.getShardMember(piece.getTable(), piece.getKey());
			transport = blockingPool.borrowObject(member);
			RococoCommunicationService.Client client = new RococoCommunicationService.Client(new TBinaryProtocol(transport));
			returnType = client.start_req(piece);
		} catch (Exception e) {
			if (member != null) {
				handleException(member.getHostName(), e);
			}
		}
		return returnType;
	}

	// public ReturnType fistRoundAsync(List<Piece> pieces) throws TException {
	// log.info("fistRoundAsync(List<Piece> pieces)");
	// List<AsyncMethodCallbackDecorator> callbacks = new
	// ArrayList<AsyncMethodCallbackDecorator>();
	// Member member = null;
	// try {
	// for (Piece p : pieces) {
	// String shardKey = p.getTable() + p.getKey();
	// member = config.getMembers(shardKey)[0]; // TODO: No replication - one
	// storage node
	// TNonblockingSocket socket = nonBlockingPool.borrowObject(member);
	// AsyncMethodCallbackDecorator callback = new
	// AsyncMethodCallbackDecorator();
	// callbacks.add(callback);
	// TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
	// RococoCommunicationService.AsyncClient AsyncClient = new
	// RococoCommunicationService.AsyncClient(protocolFactory, clientManager,
	// socket);
	// AsyncClient.start_req(p, callback);
	// }
	// for (AsyncMethodCallbackDecorator callback : callbacks) {
	// Object res = callback.getResult();
	// while (res == null) {
	// // log.info("sleep");
	// Thread.sleep(1);//TODO
	// res = callback.getResult();
	// }
	// ReturnType returnType =
	// ((RococoCommunicationService.AsyncClient.start_req_call)
	// res).getResult();
	// return returnType;//TODO
	// }
	// } catch (Exception e) {
	// if (member != null) {
	// handleException(member.getHostName(), e);
	// }
	// }
	// return null;
	// }

	// public ReturnType secondRoundAsync(Member member, List<Piece> pieces)
	// throws TException {
	// AsyncMethodCallbackDecorator callback = null;
	// try {
	// for (Piece p : pieces) {
	// // TODO
	// }
	// } catch (Exception e) {
	//
	// handleException(member.getHostName(), e);
	// }
	// return null;
	// }

	private void handleException(String target, Exception e) {
		String msg = "Error contacting the remote member: " + target;
		LOG.warn(msg, e);
	}
}