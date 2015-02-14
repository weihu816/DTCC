package cn.ict.rococo.server.coordinator.messaging;

import org.apache.thrift.async.AsyncMethodCallback;

public class AsyncMethodCallbackDecorator implements AsyncMethodCallback {

    Object response = null;

	public Object getResult() {
		return this.response;
	}

	@Override
	public void onComplete(Object o) {
		this.response = o;
	}

	@Override
	public void onError(Exception arg0) {
		
	}
	
 
}
