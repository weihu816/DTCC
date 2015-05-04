package cn.ict.rcc.server.coordinator.messaging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import cn.ict.dtcc.config.AppServerConfiguration;
import cn.ict.dtcc.config.Member;
import cn.ict.rcc.benchmark.tpcc.Chopper;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.StartResponseBulk;

public class StartListener {
	
	private static final Log LOG = LogFactory.getLog(StartListener.class);

	List<Piece> pieces;
	private AppServerConfiguration config = AppServerConfiguration.getConfiguration();
	CoordinatorCommunicator communicator;
	Chopper chopper;

	private AtomicInteger totalReceived = new AtomicInteger(0);
	private int totalSent = 0;

	
//	private Map<Integer, List<Map<String, String>>> readSet = new ConcurrentHashMap<Integer, List<Map<String, String>>>();


	public StartListener(List<Piece> pieces, Chopper chopper) {
		this.pieces = pieces;
		this.chopper = chopper;
		this.communicator =  chopper.getCommunicator();
	}
	
	public void notifyOutcome(StartResponseBulk startResponseBulk, Member member, List<Piece> pieces) {
		LOG.debug("notified by " + member.getPort());
		totalReceived.addAndGet(1);
		for (int i = 0; i < startResponseBulk.getOutput().size(); i++) {
			chopper.getReadSet().put(pieces.get(i).getId(), startResponseBulk.getOutput().get(i));
		}
		chopper.getGraph().putAll(startResponseBulk.getDep().getVertexes());
		// ServersInvolved
		Map<String, String> vertexes = startResponseBulk.getDep().getVertexes();
		for (Entry<String, String> entry : vertexes.entrySet()) {
			String key = entry.getValue();
			Set<String> set = chopper.getServersInvolvedList().get(key);
			if (set == null) {
				set = new HashSet<String>();
				chopper.getServersInvolvedList().put(key, set);
			}
			set.add(member.getId());
		}
		if (isFinished()) {
			synchronized (this) {
				notifyAll();
			}
		}
	}

	public Set<Member> start() throws TException {
		Map<Member,List<Piece>> pieceMap = new HashMap<Member, List<Piece>>();
		for (Piece p : pieces) {
			Member member = config.getShardMember(p.getTable(), p.getKey());
			List<Piece> ps = pieceMap.get(member);
			if (ps == null) {
				ps = new ArrayList<Piece>();
				pieceMap.put(member, ps);
			}
			ps.add(p);
		}
		
		for (Member member : pieceMap.keySet()) {
			List<Piece> ps = pieceMap.get(member);
			StartMethodCallback callback = new StartMethodCallback(this, member, ps);
			communicator.firstRound(member, ps, callback);
			totalSent++;
		}
		return pieceMap.keySet();
	}
	
	public boolean isFinished() {
		return (totalReceived.get() > 0 && totalReceived.get() == totalSent);
	}

	
}
