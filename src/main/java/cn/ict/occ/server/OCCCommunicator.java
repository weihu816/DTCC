package cn.ict.occ.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.StackKeyedObjectPool;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import cn.ict.dtcc.config.Member;
import cn.ict.dtcc.exception.OCCException;
import cn.ict.dtcc.messaging.AsyncMethodCallbackDecorator;
import cn.ict.dtcc.messaging.ThriftConnectionPool;
import cn.ict.dtcc.messaging.ThriftNonBlockingConnectionPool;
import cn.ict.occ.appserver.BulkVoteCounter;
import cn.ict.occ.messaging.Accept;
import cn.ict.occ.messaging.OCCCommunicationService;
import cn.ict.occ.messaging.ReadValue;

public class OCCCommunicator {

	private static final Log log = LogFactory.getLog(OCCCommunicator.class);

	private ExecutorService exec;
	private TServer server;
	private TAsyncClientManager clientManager;

	private KeyedObjectPool<Member, TTransport> blockingPool = new StackKeyedObjectPool<Member, TTransport>(
			new ThriftConnectionPool());
	private KeyedObjectPool<Member, TNonblockingSocket> nonBlockingPool = new StackKeyedObjectPool<Member, TNonblockingSocket>(
			new ThriftNonBlockingConnectionPool());

	public OCCCommunicator() {
		try {
			this.clientManager = new TAsyncClientManager();
		} catch (IOException e) {
			throw new OCCException("Failed to initialize Thrift client manager");
		}
	}

	public void stopSender() {
		clientManager.stop();
	}

	// start listener to handle incoming calls
	public void startListener(final StorageNode agent, final int port) {
		exec = Executors.newSingleThreadExecutor();
		exec.submit(new Runnable() {
			public void run() {
				try {
					TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
					OCCCommunicationService.Processor processor = new OCCCommunicationService.Processor(
							new OCCCommunicationServiceHandler(agent));
					server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
					log.info("Starting server on port: " + port);
					server.serve();
				} catch (TTransportException e) {
					log.error("Error while initializing the Thrift service", e);
				}
			}
		});
	}

	public void stopListener() {
		server.stop();
		exec.shutdownNow();
	}
	
	public ReadValue get(Member member, String table, String key, List<String> names) {
        TTransport transport = null;
        boolean error = false;
        try {
            transport = blockingPool.borrowObject(member);
            TProtocol protocol = new TBinaryProtocol(transport);
            OCCCommunicationService.Client client = new OCCCommunicationService.Client(protocol);
            return client.read(table, key, names);
        } catch (Exception e) {
            error = true;
            handleException(member.getHostName(), e);
            return null;
        } finally {
            if (transport != null) {
                try {
                    if (error) {
                        blockingPool.invalidateObject(member, transport);
                    } else {
                        blockingPool.returnObject(member, transport);
                    }
                } catch (Exception ignored) {
                }
            }
        }
	}
	
	public ReadValue getIndexFetch(Member member, String table, String keyIndex,
			List<String> names, String orderField, Boolean isAssending, String type) {
        TTransport transport = null;
        boolean error = false;
        try {
            transport = blockingPool.borrowObject(member);
            TProtocol protocol = new TBinaryProtocol(transport);
            OCCCommunicationService.Client client = new OCCCommunicationService.Client(protocol);
            if (type.equals("middle")) {
                return client.readIndexFetchMiddle(table, keyIndex, names, orderField, isAssending);
            } else {
            	// top
                return client.readIndexFetchTop(table, keyIndex, names, orderField, isAssending);
            }
        } catch (Exception e) {
            error = true;
            handleException(member.getHostName(), e);
            return null;
        } finally {
            if (transport != null) {
                try {
                    if (error) {
                        blockingPool.invalidateObject(member, transport);
                    } else {
                        blockingPool.returnObject(member, transport);
                    }
                } catch (Exception ignored) {
                }
            }
        }
	}
	
	public List<ReadValue> getIndexFetch(Member member, String table, String keyIndex,
			List<String> names) {
        TTransport transport = null;
        boolean error = false;
        try {
            transport = blockingPool.borrowObject(member);
            TProtocol protocol = new TBinaryProtocol(transport);
            OCCCommunicationService.Client client = new OCCCommunicationService.Client(protocol);
            return client.readIndexFetchAll(table, keyIndex, names);
        } catch (Exception e) {
            error = true;
            handleException(member.getHostName(), e);
            return null;
        } finally {
            if (transport != null) {
                try {
                    if (error) {
                        blockingPool.invalidateObject(member, transport);
                    } else {
                        blockingPool.returnObject(member, transport);
                    }
                } catch (Exception ignored) {
                }
            }
        }
	}
	
	public void sendBulkAcceptAsync(BulkVoteCounter counter, Member member, List<cn.ict.occ.appserver.Accept> accepts) {
		AsyncMethodCallbackDecorator callback = null;
		try {
            TNonblockingSocket socket = nonBlockingPool.borrowObject(member);
            callback = new AsyncMethodCallbackDecorator(counter, socket, member, nonBlockingPool);
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            OCCCommunicationService.AsyncClient client =
                    new OCCCommunicationService.AsyncClient(protocolFactory, clientManager, socket);
            ArrayList<Accept> tAccepts = new ArrayList<Accept>(accepts.size());
            for (cn.ict.occ.appserver.Accept accept : accepts) {
            	tAccepts.add(toThriftAccept(accept));
            }
            client.bulkAccept(tAccepts, callback);
        } catch (Exception e) {
            if (callback != null) {
                callback.onError(e);
            } else {
            	counter.onError(e);
            }
            handleException(member.getHostName(), e);
        }
	}
	
	public boolean sendDecideAsync(Member member, String transaction, boolean commit) {
        AsyncMethodCallbackDecorator callback = null;
        try {
            TNonblockingSocket socket = nonBlockingPool.borrowObject(member);
            callback = new AsyncMethodCallbackDecorator(socket, member, nonBlockingPool);
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            OCCCommunicationService.AsyncClient client =
                    new OCCCommunicationService.AsyncClient(protocolFactory, clientManager, socket);
            client.decide(transaction, commit, callback);
        } catch (Exception e) {
            if (callback != null) {
                callback.onError(e);
            }
            handleException(member.getHostName(), e);
        }
        return true;
    }

	private Accept toThriftAccept(cn.ict.occ.appserver.Accept accept) {
		return new Accept(accept.getTransactionId(), accept.getTable(),
				 accept.getKey(), accept.getNames(), accept.getValues(), accept.getOldVersion());
    }
	
	private void handleException(String target, Exception e) {
        String msg = "Error contacting the remote member: " + target;
        log.warn(msg, e);
    }
}
