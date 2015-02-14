package cn.ict.rococo.txn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import cn.ict.rococo.messaging.RococoCoordinator;

public class CoordinatorClient {

	private static final Log log = LogFactory.getLog(CoordinatorClient.class);
	
	private CoordinatorClientConfiguration configuration;
	
	public CoordinatorClient() {
        this.configuration = CoordinatorClientConfiguration.getConfiguration();
	}
	
	public boolean ping() throws TException {
		final String host = configuration.getHost();
		final int port = configuration.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
            RococoCoordinator.Client client = getClient(transport);
            return client.ping();
        } catch (TException e) {
            handleException(host, e);
            return false;
        } finally {
            close(transport);
        }
	}

	public void NewOrder(int w_id, int d_id) {
		final String host = configuration.getHost();
		final int port 	  = configuration.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
            RococoCoordinator.Client client = getClient(transport);
            client.NewOrder(w_id, d_id);
        } catch (TException e) {
        	e.printStackTrace();
            //handleException(host, e);
        } finally {
            close(transport);
        }
	}

	private RococoCoordinator.Client getClient(TTransport transport) throws TTransportException {
		if (!transport.isOpen()) {
			transport.open();
		}
		TProtocol protocol = new TBinaryProtocol(transport);
		return new RococoCoordinator.Client(protocol);
	}

	private void close(TTransport transport) {
        if (transport.isOpen()) {
            transport.close();
        }
    }
	
	private void handleException(String target, Exception e) {
        String msg = "Error contacting the remote member: " + target;
        log.warn(msg, e);
    }

}
