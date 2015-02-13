package cn.ict.rococo.coordinator.messaging;

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

import cn.ict.rococo.Member;
import cn.ict.rococo.coordinator.config.CoordinatorConfiguration;
import cn.ict.rococo.exception.RococoException;
import cn.ict.rococo.messaging.Piece;
import cn.ict.rococo.messaging.ReturnType;
import cn.ict.rococo.messaging.RococoCommunicationService;
import cn.ict.rococo.messaging.ThriftConnectionPool;
import cn.ict.rococo.messaging.ThriftNonBlockingConnectionPool;

public class CoordinatorCommunicator {

	private static final Log log = LogFactory.getLog(CoordinatorCommunicator.class);
	private CoordinatorConfiguration config;

	private KeyedObjectPool<Member, TTransport> blockingPool = new StackKeyedObjectPool<Member, TTransport>(
			new ThriftConnectionPool());
	private KeyedObjectPool<Member, TNonblockingSocket> nonBlockingPool = new StackKeyedObjectPool<Member, TNonblockingSocket>(
			new ThriftNonBlockingConnectionPool());

	private TAsyncClientManager clientManager;

	public CoordinatorCommunicator() {
		config = CoordinatorConfiguration.getConfiguration();
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
		log.info("fistRoundAsync(Piece piece)");
		Member member = null;
		TTransport transport = null;
		ReturnType returnType = null;
		try {
			String shardKey = piece.getTable() + piece.getKey();
			member = config.getMembers(shardKey)[0]; // no replication for now
			transport = blockingPool.borrowObject(member);
			RococoCommunicationService.Client client = new RococoCommunicationService.Client(
					new TBinaryProtocol(transport));
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
		log.warn(msg, e);
	}
}
