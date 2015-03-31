package cn.ict.occ.appserver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.dtcc.config.AppServerConfiguration;
import cn.ict.dtcc.config.Member;

public class VoteListener {

    private static final Log log = LogFactory.getLog(VoteListener.class);

    public static final String DEFAULT_SERVER_ID = "AppServer";

    private Queue<Option> acceptedOptions = new ConcurrentLinkedQueue<Option>();
	private Queue<Option> rejectedOptions = new ConcurrentLinkedQueue<Option>();
    private Collection<Option> options;
    private OCCCommunicator communicator;
    private String txnId;

    public VoteListener(Collection<Option> options, OCCCommunicator communicator,
                                 String txnId) {
        this.options = options;
        this.communicator = communicator;
        this.txnId = txnId;
    }

    public Set<Member> start() {
        AppServerConfiguration config = AppServerConfiguration.getConfiguration();
        Map<Member,List<Option>> optionsMap = new HashMap<Member, List<Option>>();
        Map<Member,List<Accept>> acceptsMap = new HashMap<Member, List<Accept>>();

        for (Option option : options) {
        	Member member = config.getShardMember(option.getTable(), option.getKey());
			List<Option> options = optionsMap.get(member);
			if (options == null) {
				options = new ArrayList<Option>();
				optionsMap.put(member, options);
			}
			options.add(option);

			List<Accept> accepts = acceptsMap.get(member);
			if (accepts == null) {
				accepts = new ArrayList<Accept>();
				acceptsMap.put(member, accepts);
			}
			accepts.add(new Accept(txnId, option));

        }

        for (Member member : optionsMap.keySet()) {
            List<Option> options = optionsMap.get(member);
            List<Accept> accepts = acceptsMap.get(member);
            BulkVoteCounter callback = new BulkVoteCounter(options, this);
            if (accepts.size() > 0) {
            	communicator.sendBulkAcceptAsync(callback, member, accepts);
            }
        }
        return optionsMap.keySet();
    }
	
	public int getAccepts() {
		return acceptedOptions.size();
	}
	
	public int getTotal() {
        return acceptedOptions.size() + rejectedOptions.size();
    }

	public void notifyOutcome(Option option, boolean accepted) {
		if (accepted) {
            acceptedOptions.add(option);
		} else {
            rejectedOptions.add(option);
        }
        synchronized(this) {
            this.notifyAll();
        }
    }
}
