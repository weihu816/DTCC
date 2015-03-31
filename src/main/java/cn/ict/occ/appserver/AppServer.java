package cn.ict.occ.appserver;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import cn.ict.dtcc.config.Member;
import cn.ict.dtcc.config.ServerConfiguration;
import cn.ict.occ.messaging.OCCAppServerService;
import cn.ict.occ.messaging.ReadValue;
import cn.ict.occ.messaging.Result;


public class AppServer implements AppServerService {

	private static final Log log = LogFactory.getLog(AppServer.class);
	
	private AppServerConfiguration configuration;
	private OCCCommunicator communicator;
	private TServer server;
    private ExecutorService exec;
    
    public AppServer() {
        this.configuration = AppServerConfiguration.getConfiguration();
        this.communicator = new OCCCommunicator();
	}
    
	@Override
	public boolean ping() {
		return true;
	}
	
	@Override
	public Result read(String table, String key, List<String> names) {
		Member member = configuration.getShardMember(table, key);
		ReadValue r = communicator.get(member, table, key, names);
		Map<String, String> readValues = new HashMap<String, String>();
		List<String> values = r.getValues();
		for (int i = 0; i < values.size(); i++) {
			readValues.put(names.get(i), values.get(i));
		}
		return new Result(table, key, readValues, r.getVersion());
	}
	@Override
	public Result readIndexFetchTop(String table, String keyIndex,
			String orderField, boolean isAssending) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Result readIndexFetchMiddle(String table, String keyIndex,
			String orderField, boolean isAssending) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	//start listener to handle incoming calls
    public void startListener() {
        exec = Executors.newSingleThreadExecutor();
        final AppServer appServer = this;
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
                    OCCAppServerService.Processor processor = new OCCAppServerService.Processor(
                            new AppServerServiceHandler(appServer));
                    server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).
                            processor(processor));
                    log.info("Starting server on port: " + port);
                    server.serve();
                } catch (TTransportException e) {
                    log.error("Error while initializing the Thrift service", e);
                }
            }
        });
    }
    
	@Override
	public void stop() {
		communicator.stopSender();	
	}

	public void stopListener() {
		if (server != null) {
			server.stop();
		}
		exec.shutdownNow();
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure(ServerConfiguration.getConfiguration()
				.getLogConfigFilePath());
		final AppServer server = new AppServer();
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

	@Override
	public boolean commit(String txnId, Collection<Option> options) {
		boolean success = false;
		VoteListener voteListener = new VoteListener(options, communicator, txnId);
        Set<Member> members = voteListener.start();

        synchronized (voteListener) {
            long start = System.currentTimeMillis();
        	while (voteListener.getTotal() < options.size()) {
                try {
					voteListener.wait(5000);
				} catch (InterruptedException ignored) { }

                if (System.currentTimeMillis() - start > 60000) {
                    log.warn("Transaction " + txnId + " timed out");
                    break;
                }
        	}
        }
        success = (voteListener.getAccepts() == options.size());
        // No need to call commit for read-only txns
        if (options.size() > 0) {
			for (Member member : members) {
				communicator.sendDecideAsync(member, txnId, success);
			}
        }

        if (!success && log.isDebugEnabled()) {
            log.debug("Expected accepts: " + options.size() + "; Received accepts: " + voteListener.getAccepts());
        }
        return success;
	}
}
