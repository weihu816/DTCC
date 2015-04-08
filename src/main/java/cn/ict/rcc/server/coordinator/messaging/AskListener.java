package cn.ict.rcc.server.coordinator.messaging;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import cn.ict.dtcc.config.Member;

public class AskListener {

	public static final Log LOG = LogFactory.getLog(AskListener.class);
	
	private CoordinatorCommunicator communicator;
	private Member member;
	private String transactionId;
	private int result = 0;
	
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
	
	public void notifyCommitOutcome(boolean result) {
		if (result) {
			this.result = 1;
		} else {
			this.result = 2;
		}
		synchronized (this) {
			this.notifyAll();
		}
	}

	public int getResult() {
		return result;
	}
}
