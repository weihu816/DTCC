package cn.ict.rcc.server.coordinator.messaging;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;

import cn.ict.dtcc.config.Member;

public class AskListener {
	
	private CoordinatorCommunicator communicator;
	private Member member;
	private String transactionId;
	private AtomicInteger result = new AtomicInteger(0);
	
	public AskListener (Member member, CoordinatorCommunicator communicator, String transactionId) {
		this.communicator = communicator;
		this.member = member;
		this.transactionId = transactionId;
	}
	
	public void start () {
		AskMethodCallback callback = new AskMethodCallback(this);
		try {
			communicator.rcc_ask_txnCommitting(member, transactionId, callback);
		} catch (TException e) {
			e.printStackTrace();
		}
	}
	
	public void notifyCommitOutcome(boolean response) {
		if (response) {
			result.set(1);
		} else {
			result.set(2);
		}
		synchronized (this) {
			this.notifyAll();
		}
	}

	public int getResult() {
		return result.get();
	}
}
