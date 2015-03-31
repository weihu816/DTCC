package cn.ict.occ.appserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import cn.ict.occ.messaging.OCCCommunicationService.AsyncClient.bulkAccept_call;

public class BulkVoteCounter implements AsyncMethodCallback {

	private static final Log log = LogFactory
			.getLog(BulkVoteCounter.class);

	private VoteListener callback;
	List<Option> options;

	public BulkVoteCounter(List<Option> options, VoteListener callback) {
		this.callback = callback;
		this.options = options;
	}

	public void onComplete(Object response) {
		if (response instanceof bulkAccept_call) {
			try {
				List<Boolean> results = ((bulkAccept_call) response).getResult();
				for (int i = 0; i < results.size(); i++) {
					if (results.get(i)) { onAccept(i); }
					else { onReject(i); }
				}
			} catch (TException e) {
				for (int i = 0; i < options.size(); i++) {
					onReject(i);
				}
			}
		}
	}

	public void onError(Exception exception) {
		for (int i = 0; i < options.size(); i++) {
			onReject(i);
		}
	}

	private void onAccept(int i) {
		if (log.isDebugEnabled()) {
			log.debug("Key=" + options.get(i).getKey() + " accept " + this.hashCode());
		}
		callback.notifyOutcome(options.get(i), true);
	}

	private void onReject(int i) {
		if (log.isDebugEnabled()) {
			log.info("Key=" + options.get(i).getKey() + " reject " + this.hashCode());
		}
		callback.notifyOutcome(options.get(i), false);
	}
}
