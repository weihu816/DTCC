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

import cn.ict.dtcc.config.AppServerConfiguration;
import cn.ict.dtcc.config.ServerConfiguration;
import cn.ict.rcc.messaging.RococoCoordinator;


public class Coordinator {

	private static final Log log = LogFactory.getLog(Coordinator.class);
	
	private AppServerConfiguration configuration;
    private TServer server;
    private ExecutorService exec;
    
    public Coordinator() {
        this.configuration = AppServerConfiguration.getConfiguration();
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
	
	//-----------------------------
	public static void main(String[] args) {
		
		PropertyConfigurator.configure(AppServerConfiguration.getConfiguration().getLogConfigFilePath());
		
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
