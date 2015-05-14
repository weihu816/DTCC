package cn.ict.pcc.appserver;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.server.TServer;

import cn.ict.dtcc.config.AppServerConfiguration;
import cn.ict.dtcc.config.Member;
import cn.ict.pcc.messaging.ReadValue;
import cn.ict.pcc.messaging.Result;
import cn.ict.pcc.server.PCCCommunicator;




public class AppServer {

	private static final Log log = LogFactory.getLog(AppServer.class);
	
	private AppServerConfiguration configuration;
	private PCCCommunicator communicator;
	private TServer server;
    private ExecutorService exec;
    
    public AppServer() {
        this.configuration = AppServerConfiguration.getConfiguration();
        this.communicator = new PCCCommunicator();
	}
    
	public Result read(String txnid, String table, String key, List<String> names) {
		Member member = configuration.getShardMember(table, key);
		ReadValue r = communicator.get(member, txnid, table, key, names);
		if (r.getValues().size() == 0) { return null; }
		Map<String, String> readValues = new HashMap<String, String>();
		List<String> values = r.getValues();
		for (int i = 0; i < values.size(); i++) {
			readValues.put(names.get(i), values.get(i));
		}
		return new Result(table, key, readValues);
	}
	
    public void stop() {
		communicator.stopSender();	
	}

	public void stopListener() {
		if (server != null) {
			server.stop();
		}
		exec.shutdownNow();
	}

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

                if (System.currentTimeMillis() - start > 30000) {
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
