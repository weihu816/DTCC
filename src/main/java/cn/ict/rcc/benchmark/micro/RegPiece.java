package cn.ict.rcc.benchmark.micro;

import java.util.ArrayList;
import java.util.List;

import cn.ict.dtcc.config.AppServerConfiguration;
import cn.ict.dtcc.util.DTCCUtil;
import cn.ict.rcc.messaging.Action;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.Vertex;
import cn.ict.rcc.server.coordinator.messaging.TransactionFactory;

public class RegPiece {

	public static final String TABLE1 = "table1";
	public static final String TABLE2 = "table2";
	public static final String TABLE3 = "table3";
	private AppServerConfiguration config = AppServerConfiguration.getConfiguration();

	public List<Piece> pieces = new ArrayList<Piece>();
	public List<String> serversInvolvedList = new ArrayList<String>();
	public RegPiece() {
		// piece 1
		Piece piece1 = reg_microbench_1();
		pieces.add(piece1);
		serversInvolvedList.add(config.getShardMember(piece1.getTable(), piece1.getKey()).getId());
	}
	
	private String transactionId = String.valueOf(TransactionFactory.transactionIdGen.addAndGet(1));

	public Piece reg_microbench_1() {
		Vertex v = null;		
		Piece piece = new Piece(new ArrayList<Vertex>(), transactionId, TABLE1, "myKey", true);
		// read
		v = new Vertex(Action.READSELECT);
		v.setName(DTCCUtil.buildColumns("myValue"));
		piece.getVertexs().add(v);
		// add 1
		v = new Vertex(Action.ADDINTEGER);
		v.setName(DTCCUtil.buildColumns("myValue"));
		v.setValue(DTCCUtil.buildColumns("1"));
		piece.getVertexs().add(v);
		return piece;
//		t1.completePiece();
//		String key1 = t1.get(num_piece1, "myValue");
	}
	
	public Piece reg_microbench_2(String[] input) {
		Vertex v = null;		
		Piece piece = new Piece(new ArrayList<Vertex>(), transactionId, TABLE1, input[0], true);
		// read
		v = new Vertex(Action.READSELECT);
		v.setName(DTCCUtil.buildColumns("myValue"));
		piece.getVertexs().add(v);
		// add 1
		v = new Vertex(Action.ADDINTEGER);
		v.setName(DTCCUtil.buildColumns("myValue"));
		v.setValue(DTCCUtil.buildColumns("1"));
		piece.getVertexs().add(v);
		
		return piece;
//		t1.completePiece();
//		String key1 = t1.get(num_piece1, "myValue");
	}
	
}
