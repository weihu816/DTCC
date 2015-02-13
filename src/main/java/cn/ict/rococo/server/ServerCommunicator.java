package cn.ict.rococo.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import cn.ict.rococo.messaging.RococoCommunicationService;

public class ServerCommunicator {

	private static final Log log = LogFactory.getLog(ServerCommunicator.class);

	private ExecutorService exec;
	private TServer server;


	public void startListener(final AgentService agent, final int port) {
		exec = Executors.newSingleThreadExecutor();
		exec.submit(new Runnable() {
			public void run() {
				try {
					TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
					RococoCommunicationService.Processor processor = new RococoCommunicationService.Processor(new ServerCommunicationServiceHandler(agent));
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
	
}
