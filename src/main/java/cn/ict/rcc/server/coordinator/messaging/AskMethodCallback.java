package cn.ict.rcc.server.coordinator.messaging;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import cn.ict.rcc.messaging.RococoCommunicationService;
import cn.ict.rcc.messaging.RococoCommunicationService.AsyncClient.commit_req_call;

public class AskMethodCallback implements AsyncMethodCallback {
	
//	private static final Log LOG = LogFactory.getLog(CommitMethodCallback.class);
	
	private AskListener callback;
	
	public AskMethodCallback(AskListener callbackListener) {
		this.callback = callbackListener;
	}
	

	@Override
	public void onComplete(Object response) {

		if (response instanceof commit_req_call) {
			try {
				boolean result = ((RococoCommunicationService.AsyncClient.rcc_ask_txnCommitting_call) response).getResult();
				callback.notifyCommitOutcome(result);
			} catch (TException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public synchronized void onError(Exception arg0) {

	}

}