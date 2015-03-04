package cn.ict.rcc.server.coordinator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import cn.ict.rcc.benchmark.tpcc.TPCC;
import cn.ict.rcc.messaging.RococoCoordinator;
import cn.ict.rcc.procedure.TransactionException;
import cn.ict.rcc.server.config.TpccServerConfiguration;


public class Coordinator {

	private static final Log log = LogFactory.getLog(Coordinator.class);
	
	private TpccServerConfiguration configuration;
    private TServer server;
    private ExecutorService exec;
    
    public Coordinator() {
        this.configuration = TpccServerConfiguration.getConfiguration();
	}
    
	public void startListener() {
        exec = Executors.newSingleThreadExecutor();
        String appServerURL = configuration.getAppServerUrl();
        if (appServerURL == null) {
        	log.error("AppServerURL not specified");
        	return;
        }
        final int port = Integer.parseInt(appServerURL.substring(appServerURL.indexOf(':') + 1));
        exec.submit(new Runnable() {
            public void run() {
                try {
                    TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
                    RococoCoordinator.Processor processor = new RococoCoordinator.Processor(new CoordinatorServiceHandler());
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
    	if (server != null) {
    		server.stop();
    	}
        exec.shutdownNow();
    }
	
    public void stop() {
    	
	}
    
//    @Override
//	public Map<String, String> accept(String transactionId, Piece piece) {
//    	log.info("accept(String transactionId, Piece piece)");
//    	ReturnType returnType = null;
//    	try {
//			returnType = communicator.fistRoundAsync(piece);
//			if (returnType == null) {
//				return null;
//			}
//		} catch (TException e) {
//			e.printStackTrace();
//		}
//		return returnType.getOutput();
//	}
//    
//	@Override
//	public Map<String, String> accept(String transactionId, List<Piece> pieces) {
//		log.info("accept(String transactionId, List<Piece> pieces)");
//		ReturnType returnType = null;
//    	try {
//			returnType = communicator.fistRoundAsync(pieces);
//			if (returnType == null) {
//				return null;
//			}
//		} catch (TException e) {
//			e.printStackTrace();
//		}
//		return returnType.getOutput();
//	}

	
	
	//-----------------------------
	public static void main(String[] args) {
		
		PropertyConfigurator.configure(TpccServerConfiguration.getConfiguration().getLogConfigFilePath());
		
		final Coordinator server = new Coordinator();
		Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
            	log.info("Shutting Down AppServer");
            	server.stop();
            	server.stopListener();
            }
		});
		server.startListener();
		
	}

	

	

}