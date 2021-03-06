package cn.ict.rcc.server.coordinator.messaging;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import cn.ict.rcc.messaging.CommitResponse;
import cn.ict.rcc.messaging.RococoCommunicationService;
import cn.ict.rcc.messaging.RococoCommunicationService.AsyncClient.commit_req_call;

public class CommitMethodCallback implements AsyncMethodCallback {
	
//	private static final Log LOG = LogFactory.getLog(CommitMethodCallback.class);
	
	private CommitListener callback;
	
	public CommitMethodCallback(CommitListener callbackListener) {
		this.callback = callbackListener;
	}
	

	@Override
	public void onComplete(Object response) {

		if (response instanceof commit_req_call) {
			try {
				CommitResponse commitResponse = ((RococoCommunicationService.AsyncClient.commit_req_call) response).getResult();
				callback.notifyCommitOutcome(commitResponse);

			} catch (TException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public synchronized void onError(Exception arg0) {

	}

}