package cn.ict.rcc.server.coordinator.messaging;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import cn.ict.rcc.messaging.RococoCommunicationService;
import cn.ict.rcc.messaging.RococoCommunicationService.AsyncClient.start_req_call;
import cn.ict.rcc.messaging.StartResponse;

public class StartMethodCallback implements AsyncMethodCallback {
	
	public static final Log LOG = LogFactory.getLog(StartMethodCallback.class);
	
	private StartListener callback;
	private int piece_number;
	public StartMethodCallback(StartListener callbackListener, int piece_number) {
		this.callback = callback;
		this.piece_number = piece_number;
	}

	@Override
	public synchronized void onComplete(Object response) {
		StartResponse startResponse = null;
		if (response instanceof start_req_call) {
			try {
				startResponse = ((RococoCommunicationService.AsyncClient.start_req_call) response).getResult();
				callback.notifyOutcome(startResponse, piece_number);
			} catch (TException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public synchronized void onError(Exception arg0) {

	}

}