package cn.ict.rcc.server.coordinator.txn;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import cn.ict.rcc.messaging.RococoCoordinator;

public class CoordinatorClient {

	public static final String MICRO_BENCHMARK = "MicroBenchmark";
	public static final String TPCC_BENCHMARK = "TPCCBenchmark";
	public static final String FUNDS_BENCHMARK = "FundsBenchmark";
	public static final String TPCC_NEWORDER = "TPCC_newOrder";
	public static final String TPCC_PAYMENT = "TPCC_payment";
	public static final String TPCC_ORDERSTATUS = "TPCC_orderStatus";
	public static final String TPCC_DELIVERY = "TPCC_delivery";
	public static final String TPCC_STOCKLEVEL = "TPCC_stockLevel";
	
	private static final Log log = LogFactory.getLog(CoordinatorClient.class);
	
	private CoordinatorClientConfiguration configuration;
	
	public static CoordinatorClient coordinatorClient = null;
	
	public CoordinatorClient() {
        this.configuration = CoordinatorClientConfiguration.getConfiguration();
	}

	public static CoordinatorClient getCoordinatorClient() {
		coordinatorClient = new CoordinatorClient();
		return coordinatorClient;
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

	public void callProcedure(String procedure, List<String> paras) {
		final String host = configuration.getHost();
		final int port 	  = configuration.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
            RococoCoordinator.Client client = getClient(transport);
            client.callProcedure(procedure, paras);
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
