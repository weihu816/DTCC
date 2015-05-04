package cn.ict.rcc.server.coordinator.messaging;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import cn.ict.dtcc.config.Member;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.RococoCommunicationService;
import cn.ict.rcc.messaging.RococoCommunicationService.AsyncClient.start_req_bulk_call;
import cn.ict.rcc.messaging.StartResponseBulk;

public class StartMethodCallback implements AsyncMethodCallback {
	
	public static final Log LOG = LogFactory.getLog(StartMethodCallback.class);
	
	private StartListener callback;
	private Member member;
	private List<Piece> pieces;
	
	public StartMethodCallback(StartListener callbackListener, Member member, List<Piece> pieces) {
		this.callback = callbackListener;
		this.member = member;
		this.pieces = pieces;
	}

	@Override
	public synchronized void onComplete(Object response) {
		StartResponseBulk startResponseBulk = null;
		if (response instanceof start_req_bulk_call) {
			try {
				startResponseBulk = ((RococoCommunicationService.AsyncClient.start_req_bulk_call) response).getResult();
				callback.notifyOutcome(startResponseBulk, member, pieces);
			} catch (TException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public synchronized void onError(Exception arg0) {

	}

}