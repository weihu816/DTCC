package cn.ict.rcc.server.coordinator.messaging;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import cn.ict.rcc.messaging.ReturnType;
import cn.ict.rcc.messaging.RococoCommunicationService;

public class MethodCallback implements AsyncMethodCallback<Object> {
	
	private static final Log LOG = LogFactory.getLog(MethodCallback.class);
	
	Object response = null;
	boolean done = false;

	public synchronized ReturnType getResult() throws TException {
		if (!done) {
			try {
				wait();
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
		}
		ReturnType returnType = ((RococoCommunicationService.AsyncClient.start_req_call) response)
				.getResult();
		return returnType;	
	}

	@Override
	public synchronized void onComplete(Object response) {
		this.response = response;
		done = true;
		notify();
	}

	@Override
	public synchronized void onError(Exception arg0) {

	}

	public synchronized boolean done() {
		return done;
	}
}