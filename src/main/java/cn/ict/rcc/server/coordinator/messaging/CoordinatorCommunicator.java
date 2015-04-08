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
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransport;

import cn.ict.dtcc.config.Member;
import cn.ict.dtcc.exception.DTCCException;
import cn.ict.dtcc.messaging.ThriftConnectionPool;
import cn.ict.dtcc.messaging.ThriftNonBlockingConnectionPool;
import cn.ict.rcc.messaging.Graph;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.RococoCommunicationService;
import cn.ict.rcc.messaging.StartResponse;

/**
 * @author Wei
 */

public class CoordinatorCommunicator {

	public static volatile CoordinatorCommunicator communicator = null;
	
	private static final Log LOG = LogFactory.getLog(CoordinatorCommunicator.class);

	private KeyedObjectPool<Member, TTransport> blockingPool = new StackKeyedObjectPool<Member, TTransport>(
			new ThriftConnectionPool());
	private KeyedObjectPool<Member, TNonblockingSocket> nonBlockingPool = new StackKeyedObjectPool<Member, TNonblockingSocket>(
			new ThriftNonBlockingConnectionPool());
//	private HashMap<Member, RococoCommunicationService.AsyncClient> asyncClientPool = new HashMap<Member, RococoCommunicationService.AsyncClient>();
//	private HashMap<Member, RococoCommunicationService.Client> clientPool = new HashMap<Member, RococoCommunicationService.Client>();

	private TAsyncClientManager clientManager;

	public CoordinatorCommunicator() {
		try {
			this.clientManager = new TAsyncClientManager();
		} catch (IOException e) {
			throw new DTCCException("Failed to initialize Thrift client manager");
		}
	}
	
	public StartResponse fistRound(Member member, Piece piece) {
		LOG.debug("fistRound Txn:" + piece.getTransactionId() + " " + piece.getTable() + " " + piece.getKey());
		TTransport transport = null;
		RococoCommunicationService.Client client = null;
        boolean error = false;

		try {
			transport = blockingPool.borrowObject(member);
            TProtocol protocol = new TBinaryProtocol(transport);
			client = new RococoCommunicationService.Client(protocol);
			return client.start_req(piece);
		} catch (Exception e) {
			error = true;
			if (member != null) {
				handleException(member.getHostName(), e);
			}
		} finally {
            if (transport != null) {
                try {
                    if (error) {
                        blockingPool.invalidateObject(member, transport);
                    } else {
                        blockingPool.returnObject(member, transport);
                    }
                } catch (Exception ignored) { }
            }
        }
		return null;
	}
	
	
	public void fistRound(Member member, Piece piece, StartMethodCallback callback) throws TException {
		LOG.debug("fistRound Txn:" + piece.getTransactionId() + " " + piece.getTable() + " " + piece.getKey());
		TNonblockingSocket transport = null;
		RococoCommunicationService.AsyncClient asyncClient = null;
		AsyncMethodCallbackDecorator asyncMethodCallback = null;
		boolean error = false;
		try {
			asyncMethodCallback = new AsyncMethodCallbackDecorator(callback, transport, member, nonBlockingPool);
			transport = nonBlockingPool.borrowObject(member);
			TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
			asyncClient = new RococoCommunicationService.AsyncClient(protocolFactory, clientManager, transport);
//			RococoCommunicationService.AsyncClient asyncClient = asyncClientPool.get(member);
//			if (asyncClient == null) {
//				asyncClientPool.put(member, asyncClient);
//			}
			asyncClient.start_req(piece, asyncMethodCallback);
		} catch (Exception e) {
			if (member != null) {
				handleException(member.getHostName(), e);
			}
		} finally {
            if (transport != null) {
                try {
                    if (error) {
                        blockingPool.invalidateObject(member, transport);
                    } else {
                        blockingPool.returnObject(member, transport);
                    }
                } catch (Exception ignored) { }
            }
        }
	}
	
	public void fistRound(Member member, List<Piece> pieces, StartMethodCallback callback) throws TException {
		LOG.debug("fistRound Txn bulk.");
		TNonblockingSocket transport = null;
		RococoCommunicationService.AsyncClient asyncClient = null;
		AsyncMethodCallbackDecorator asyncMethodCallback = null;
		boolean error = false;
		try {
			asyncMethodCallback = new AsyncMethodCallbackDecorator(callback, transport, member, nonBlockingPool);
			transport = nonBlockingPool.borrowObject(member);
			TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
			asyncClient = new RococoCommunicationService.AsyncClient(protocolFactory, clientManager, transport);
			asyncClient.start_req_bulk(pieces, asyncMethodCallback);
		} catch (Exception e) {
			if (member != null) {
				handleException(member.getHostName(), e);
			}
		} finally {
            if (transport != null) {
                try {
                    if (error) {
                        blockingPool.invalidateObject(member, transport);
                    } else {
                        blockingPool.returnObject(member, transport);
                    }
                } catch (Exception ignored) { }
            }
        }
	}
	
	
	public void secondRound(Member member, String transactionId,
			Graph dep, CommitMethodCallback callback) throws TException {
		LOG.debug("secondRound Txn: " + transactionId + " " + dep);
		TNonblockingSocket transport = null;
		AsyncMethodCallbackDecorator asyncMethodCallback = null;
		RococoCommunicationService.AsyncClient asyncClient = null;
		boolean error = false;
		try {
			asyncMethodCallback = new AsyncMethodCallbackDecorator(callback, transport, member, nonBlockingPool);
			transport = nonBlockingPool.borrowObject(member);
			TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
			asyncClient = new RococoCommunicationService.AsyncClient(protocolFactory, clientManager, transport);
			asyncClient.commit_req(transactionId, dep, asyncMethodCallback);
		}
		catch (Exception e) {
			error = true;
			if (member != null) {
				handleException(member.getHostName(), e);
			}
		} finally {
			if (transport != null) {
				try {
					if (error) { blockingPool.invalidateObject(member, transport); }
					else { blockingPool.returnObject(member, transport); }
				} catch (Exception ignored) { } // ignored
			}
		}
	}

	public void rcc_ask_txnCommitting(Member member, String transactionId, AskMethodCallback callback) throws TException {
		LOG.debug("secondRound Txn: " + transactionId);
		TNonblockingSocket transport = null;
		AsyncMethodCallbackDecorator asyncMethodCallback = null;
		RococoCommunicationService.AsyncClient asyncClient = null;
		boolean error = false;
		try {
			asyncMethodCallback = new AsyncMethodCallbackDecorator(callback, transport, member, nonBlockingPool);
			transport = nonBlockingPool.borrowObject(member);
			TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
			asyncClient = new RococoCommunicationService.AsyncClient(protocolFactory, clientManager, transport);
			asyncClient.rcc_ask_txnCommitting(transactionId, asyncMethodCallback);
		}
		catch (Exception e) {
			error = true;
			if (member != null) {
				handleException(member.getHostName(), e);
			}
		} finally {
			if (transport != null) {
				try {
					if (error) { blockingPool.invalidateObject(member, transport); }
					else { blockingPool.returnObject(member, transport); }
				} catch (Exception ignored) { } // ignored
			}
		}
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
