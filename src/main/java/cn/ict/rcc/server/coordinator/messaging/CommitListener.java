package cn.ict.rcc.server.coordinator.messaging;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import cn.ict.dtcc.config.Member;
import cn.ict.rcc.messaging.Graph;

public class CommitListener {

	public static final Log LOG = LogFactory.getLog(CommitListener.class);
	
	private CoordinatorCommunicator communicator;
	private Set<Member> members;
	private String transactionId;
	private Graph graph;
	private AtomicInteger count = new AtomicInteger(0);
	
	public CommitListener (Set<Member> members, CoordinatorCommunicator communicator, String transactionId, Graph graph) {
		this.communicator = communicator;
		this.members = members;
		this.transactionId = transactionId;
		this.graph = graph;
	}
	
	public void start () {
		
		for (Member member : members) {
			CommitMethodCallback callback = new CommitMethodCallback(this);
			try {
				communicator.secondRound(member, transactionId, graph, callback);
			} catch (TException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void notifyCommitOutcome(boolean returnType) {
		if (returnType) {
			count.incrementAndGet();
		}
		if (getCount() == members.size()) {
			synchronized (this) {
				this.notifyAll();
			}
		}
	}

	public int getCount() {
		return count.get();
	}
}
