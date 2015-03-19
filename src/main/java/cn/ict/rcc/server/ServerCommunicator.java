package cn.ict.rcc.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import cn.ict.rcc.messaging.RococoCommunicationService;

public class ServerCommunicator {

	private static final Log log = LogFactory.getLog(ServerCommunicator.class);

	private ExecutorService exec;
	private TServer server;


	public void startListener(final StorageNode node, final int port) {
		exec = Executors.newSingleThreadExecutor();
		exec.submit(new Runnable() {
			public void run() {
				try {
//					TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
//					RococoCommunicationService.Processor processor = 
//							new RococoCommunicationService.Processor(new ServerCommunicationServiceHandler(node));
//					server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));

			         
					TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
					RococoCommunicationService.Processor processor = 
							new RococoCommunicationService.Processor(new ServerCommunicationServiceHandler(node));
					TThreadedSelectorServer.Args thhsArgs = new TThreadedSelectorServer.Args(socket);
					thhsArgs.processor(processor);
					thhsArgs.transportFactory(new TFramedTransport.Factory());
					thhsArgs.protocolFactory(new TBinaryProtocol.Factory());
					TServer server = new TThreadedSelectorServer(thhsArgs);
			        
//					TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
//					RococoCommunicationService.Processor processor = 
//							new RococoCommunicationService.Processor(new ServerCommunicationServiceHandler(node));
//					THsHaServer.Args arg = new THsHaServer.Args(serverTransport);
//				    arg.protocolFactory(new TBinaryProtocol.Factory());
//				    arg.transportFactory(new TFramedTransport.Factory());
//				    arg.processorFactory(new TProcessorFactory(processor));
//				    TServer server=new THsHaServer(arg);
				    
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
