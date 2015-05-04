package cn.ict.rcc.benchmark.tpcc;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import cn.ict.dtcc.benchmark.tpcc.TPCCConstants;
import cn.ict.dtcc.benchmark.tpcc.TPCCGenerator;
import cn.ict.dtcc.config.Member;
import cn.ict.rcc.messaging.Action;
import cn.ict.rcc.messaging.Graph;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.Vertex;
import cn.ict.rcc.server.coordinator.messaging.CommitListener;
import cn.ict.rcc.server.coordinator.messaging.CoordinatorCommunicator;
import cn.ict.rcc.server.coordinator.messaging.StartListener;
import cn.ict.rcc.server.coordinator.messaging.TransactionFactory;

public class Chopper_neworder implements Chopper {

	public static final int TPCC_NEW_ORDER_0 = 0;
	public static final int TPCC_NEW_ORDER_1 = 100;
	public static final int TPCC_NEW_ORDER_2 = 200;
	public static final int TPCC_NEW_ORDER_3 = 300;
	public static final int TPCC_NEW_ORDER_4 = 400;
	public static final int TPCC_NEW_ORDER_5 = 500;
	public static final int TPCC_NEW_ORDER_6 = 600;
	public static final int TPCC_NEW_ORDER_7 = 700;
	public static final int TPCC_NEW_ORDER_8 = 800;
	
	private static final Log LOG = LogFactory.getLog(Chopper_neworder.class);

	private String transactionId = UUID.randomUUID().toString();
//	private String transactionId = String.valueOf(TransactionFactory.transactionIdGen.addAndGet(1));
	private CoordinatorCommunicator communicator;
	private Map<String, String> dep = new ConcurrentHashMap<String, String>();
	private Map<String, Set<String>> serversInvolvedList = new ConcurrentHashMap<String, Set<String>>();
	private Map<Integer, List<String>> readSet = new ConcurrentHashMap<Integer, List<String>>();
	
	Set<Member> members = new HashSet<Member>();

	
	int pieceNum = 0;
	
	int w_id, d_id, c_id;
	int o_all_local, o_ol_cnt;
	String o_entry_d;
	String[] i_data, i_names;
	float[] ol_amounts;
	int[] ol_i_ids, supware, ol_quantities;

	//-----
	int o_id;
	float w_tax, d_tax, c_discount;
	float[] i_prices;

	String c_last, c_credit;
	
	String[] ol_dist_info;
	int[] s_quantities;
	char[] bg;

	/*
	 * initiate
	 */
	public Chopper_neworder(int w_id, int d_id, CoordinatorCommunicator communicator) {
		this.communicator = communicator;
		this.w_id = w_id;
		this.d_id = d_id;
		c_id = TPCCGenerator.NURand(TPCCConstants.A_C_ID, 1, TPCCConstants.CUSTOMERS_PER_DISTRICT);
		o_all_local = 1;
		o_ol_cnt = TPCCGenerator.randomInt(5, 15);
		supware = new int[o_ol_cnt];
		ol_i_ids = new int[o_ol_cnt];
		i_names = new String[o_ol_cnt];
		i_data = new String[o_ol_cnt];
		i_prices = new float[o_ol_cnt];
		ol_amounts = new float[o_ol_cnt];
		ol_quantities = new int[o_ol_cnt];
		s_quantities = new int[o_ol_cnt];
		ol_dist_info = new String[o_ol_cnt];
		bg = new char[o_ol_cnt];
		Set<Integer> set_ol_i_ids = new HashSet<Integer>();
		while (set_ol_i_ids.size() != o_ol_cnt)
			set_ol_i_ids.add(TPCCGenerator.NURand(TPCCConstants.A_OL_I_ID, 1, TPCCConstants.NUM_ITEMS));
		Object[] arr_ol_i_ids = set_ol_i_ids.toArray();
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
			int ol_supply_w_id;
			/* 90% of supply are from home stock */
			if (TPCCGenerator.randomInt(0, 99) < 0 && TPCCConstants.NUM_WAREHOUSE > 1) {
				int supply_w_id = TPCCGenerator.randomInt(1,TPCCConstants.NUM_WAREHOUSE);
				while (supply_w_id == w_id) {
					supply_w_id = TPCCGenerator.randomInt(1,TPCCConstants.NUM_WAREHOUSE);
				}
				ol_supply_w_id = supply_w_id;
			} else { ol_supply_w_id = w_id; }
			if (ol_supply_w_id != w_id) { o_all_local = 0; }
			supware[ol_number - 1] = ol_supply_w_id;
			ol_i_ids[ol_number - 1] = (int) arr_ol_i_ids[ol_number - 1];
			ol_quantities[ol_number - 1] = TPCCGenerator.randomInt(1, 10);
		}
		o_entry_d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(System.currentTimeMillis()));
	}

	// RW district I
	public Piece TPCC_NEW_ORDER_0() {
		String key_district = TPCCGenerator.buildString(w_id, "_", d_id);		
		Piece piece = new Piece(new ArrayList<Vertex>(), transactionId, TPCCConstants.TABLENAME_DISTRICT, key_district, true, TPCC_NEW_ORDER_0);

		Vertex v;
		
		// read
		v = new Vertex(Action.READSELECT);
		v.setName(TPCCGenerator.buildColumns("d_next_o_id", "d_tax"));
		piece.getVertexs().add(v);
		// update
		v = new Vertex(Action.ADDI);
		v.setName(TPCCGenerator.buildColumns("d_next_o_id"));
		v.setValue(TPCCGenerator.buildColumns("1"));
		piece.getVertexs().add(v);

//		int d_next_o_id = Integer.valueOf(transaction.get(pieceNum_district, "d_next_o_id"));
//		float d_tax = Float.valueOf(transaction.get(pieceNum_district, "d_tax"));
		return piece;
	}
	
	// R warehouse I : w_tax
	public Piece TPCC_NEW_ORDER_1() {
		String key_warehouse = String.valueOf(w_id);
		Piece piece = new Piece(new ArrayList<Vertex>(), transactionId, TPCCConstants.TABLENAME_WAREHOUSE, key_warehouse, true, TPCC_NEW_ORDER_1);
		Vertex v;
		
		// read
		v = new Vertex(Action.READSELECT);
		v.setName(TPCCGenerator.buildColumns("w_tax"));
		piece.getVertexs().add(v);

//		float w_tax = Float.valueOf(transaction.get(pieceNum_warehouse, "w_tax"));
		return piece;
	}
	
	// R customer I : 
	public Piece TPCC_NEW_ORDER_2() {
		String key_customer = TPCCGenerator.buildString(w_id, "_", d_id, "_", c_id);
		Piece piece = new Piece(new ArrayList<Vertex>(), transactionId, TPCCConstants.TABLENAME_CUSTOMER, key_customer, true, TPCC_NEW_ORDER_2);
		Vertex v;

		// read
		v = new Vertex(Action.READSELECT);
		v.setName(TPCCGenerator.buildColumns ("c_last", "c_discount", "c_credit"));
		piece.getVertexs().add(v);

//		float  c_discount 	= Float.valueOf(transaction.get(pieceNum_customer, "c_discount"));
//		String c_last 		= transaction.get(pieceNum_customer, "c_last");
//		String c_credit 	= transaction.get(pieceNum_customer, "c_credit");
		return piece;
	}
	
	// W order D:
	public Piece TPCC_NEW_ORDER_3(int o_id) {
		String key_order = TPCCGenerator.buildString(w_id, "_", d_id, "_", o_id);
		Piece piece = new Piece(new ArrayList<Vertex>(), transactionId, TPCCConstants.TABLENAME_ORDER, key_order, false, TPCC_NEW_ORDER_3);
		Vertex v;
		
		v = new Vertex(Action.WRITE);
		v.setName(TPCCGenerator.buildColumns("o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_id", "o_carrier_id", "o_ol_cnt", "o_all_local"));
		v.setValue(TPCCGenerator.buildColumns(o_id, d_id, w_id, c_id, o_entry_d, "NULL", o_ol_cnt, o_all_local));
		piece.getVertexs().add(v);
		
		return piece;
	}
	
	// W neworder D:
	public Piece TPCC_NEW_ORDER_4(int o_id) {
		String key_newOrder = TPCCGenerator.buildString(w_id, "_", d_id, "_", o_id);
		Piece piece = new Piece(new ArrayList<Vertex>(), transactionId, TPCCConstants.TABLENAME_NEW_ORDER, key_newOrder, false, TPCC_NEW_ORDER_4);
		Vertex v;

		v = new Vertex(Action.WRITE);
		v.setName(TPCCGenerator.buildColumns("no_o_id", "no_d_id", "no_w_id"));
		v.setValue(TPCCGenerator.buildColumns(o_id, d_id, w_id));
		piece.getVertexs().add(v);

		return piece;
	}
	
	// R item I:
	public List<Piece> TPCC_NEW_ORDER_5() {
		List<Piece> pieces = new ArrayList<Piece>();
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
			String key_item = String.valueOf(ol_i_ids[ol_number - 1]);
			Piece piece = new Piece(new ArrayList<Vertex>(), transactionId, TPCCConstants.TABLENAME_ITEM, key_item, true, TPCC_NEW_ORDER_5 + ol_number - 1);
			Vertex v;

			v = new Vertex(Action.READSELECT);
			v.setName(TPCCGenerator.buildColumns("i_price", "i_name", "i_data"));
			piece.getVertexs().add(v);

			pieces.add(piece);
//			String i_name = transaction.get(pieceNum_item, "i_name");
//			float i_price = Float.valueOf(transaction.get(pieceNum_item, "i_price"));
//			String i_data = transaction.get(pieceNum_item, "i_data");
//			i_names			[ol_number - 1] = i_name;
//			i_prices		[ol_number - 1] = i_price;
//			i_datas			[ol_number - 1] = i_data;
		}
		return pieces;
	}
	
	// R stock I:
	public List<Piece> TPCC_NEW_ORDER_6() {
		List<Piece> pieces = new ArrayList<Piece>();
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
			String key_stock = TPCCGenerator.buildString(supware[ol_number - 1], "_", ol_i_ids[ol_number - 1]);
			Piece piece = new Piece(new ArrayList<Vertex>(), transactionId, TPCCConstants.TABLENAME_STOCK, key_stock, true, TPCC_NEW_ORDER_6 + ol_number - 1);
			Vertex v;
			
			v = new Vertex(Action.READSELECT);
			v.setName(TPCCGenerator.buildColumns("s_data", "s_dist_" +  d_id));
			piece.getVertexs().add(v);
			
			pieces.add(piece);

//			String ol_dist_info = transaction.get(pieceNum_stock, "s_dist_" +  d_id);
//			String s_data = transaction.get(pieceNum_stock, "s_data");
		}
		return pieces;
	}
	
	
	// W stock D
	public List<Piece> TPCC_NEW_ORDER_7() {
		List<Piece> pieces = new ArrayList<Piece>();
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
			String key_stock = TPCCGenerator.buildString(supware[ol_number - 1], "_", ol_i_ids[ol_number - 1]);	
			Piece piece = new Piece(new ArrayList<Vertex>(), transactionId, TPCCConstants.TABLENAME_STOCK, key_stock, false, TPCC_NEW_ORDER_7 + ol_number - 1);
			Vertex v;
			// update
			v = new Vertex(Action.REDUCEI);
			v.setName(TPCCGenerator.buildColumns("s_quantity"));
			v.setValue(TPCCGenerator.buildColumns(ol_quantities[ol_number - 1], 10, 91));
			piece.getVertexs().add(v);
			
			v = new Vertex(Action.READSELECT);
			v.setName(TPCCGenerator.buildColumns("s_quantity"));
			piece.getVertexs().add(v);
			// TODO!!!!!!!!! another problem ... how to detect conflicts
			pieces.add(piece);
		}
		return pieces;
	}

	// W orderline D
	public List<Piece> TPCC_NEW_ORDER_8(float w_tax, float d_tax, int o_id, float c_discount, float[] i_prices, String[] ol_dist_info) {
		List<Piece> pieces = new ArrayList<Piece>();
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
			float ol_amount = ol_quantities[ol_number-1] * i_prices[ol_number-1] *(1 + w_tax + d_tax) * (1-c_discount); 
			ol_amounts [ol_number - 1] = ol_amount;

			String key_orderline = TPCCGenerator.buildString(w_id, "_", d_id , "_" , o_id , "_", ol_number);
			Piece piece = new Piece(new ArrayList<Vertex>(), transactionId, TPCCConstants.TABLENAME_ORDER_LINE, key_orderline, false, TPCC_NEW_ORDER_8 + ol_number - 1);
			Vertex v;
			
			// write
			v = new Vertex(Action.WRITE);
			v.setName(TPCCGenerator.buildColumns("ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d",
					"ol_quantity", "ol_amount", "ol_dist_info"));
			v.setValue(TPCCGenerator.buildColumns(o_id, d_id, w_id, ol_number, ol_i_ids[ol_number - 1],
					supware[ol_number - 1], "NULL", ol_quantities[ol_number-1], ol_amount, ol_dist_info[ol_number-1]));
			piece.getVertexs().add(v);
			
			pieces.add(piece);
		}
		return pieces;
	}

	
// 0 1 2 5 6 7
// 0 - 3 4
// 0 1 2 5 6 - 8
	public boolean run() throws TException {
		List<Piece> pieces;
		
		//----------------------------------------------------------------
		pieces = new ArrayList<Piece>();
		pieces.add(TPCC_NEW_ORDER_0());
		pieces.add(TPCC_NEW_ORDER_1());
		pieces.add(TPCC_NEW_ORDER_2());
		pieces.addAll(TPCC_NEW_ORDER_5());
		pieces.addAll(TPCC_NEW_ORDER_6());
		pieces.addAll(TPCC_NEW_ORDER_7());

        StartListener startListener;
        
		startListener = new StartListener(pieces, this);
        members.addAll(startListener.start());
        synchronized (startListener) {
            long start = System.currentTimeMillis();
			while (!startListener.isFinished()) {
				try {
			        LOG.warn("neworder waiting 1");
					startListener.wait(1000);
				} catch (InterruptedException ignored) { }
				if (System.currentTimeMillis() - start > 10000) {
	                LOG.warn("Pieces timed out");
	                break;
	            }
			}
		}
        
        LOG.debug(readSet);
        LOG.debug(dep);
        LOG.debug(serversInvolvedList);
		//----------------------------------------------------------------

       
        // TPCC_NEW_ORDER_0
        o_id = Integer.parseInt(readSet.get(TPCC_NEW_ORDER_0).get(0));
        d_tax = Float.parseFloat(readSet.get(TPCC_NEW_ORDER_0).get(1));
        LOG.debug("o_id=" + o_id);
        LOG.debug("d_tax=" + d_tax);
        // TPCC_NEW_ORDER_1
        w_tax = Float.parseFloat(readSet.get(TPCC_NEW_ORDER_1).get(0));
        LOG.debug("w_tax=" + w_tax);
        // TPCC_NEW_ORDER_2
        c_last = readSet.get(TPCC_NEW_ORDER_2).get(0);
        c_discount = Float.parseFloat(readSet.get(TPCC_NEW_ORDER_2).get(1));
        c_credit = readSet.get(TPCC_NEW_ORDER_2).get(2);
        LOG.debug("c_last=" + c_last);
        LOG.debug("c_discount=" + c_discount);
        LOG.debug("c_credit=" + c_credit);
        // TPCC_NEW_ORDER_3
        for (int i = 0; i < o_ol_cnt; i++) {
    		i_prices [i] = Float.parseFloat(readSet.get(TPCC_NEW_ORDER_5+i).get(0));
        	i_names	[i] = readSet.get(TPCC_NEW_ORDER_5+i).get(1);
    		String i_data = readSet.get(TPCC_NEW_ORDER_5+i).get(2);

    		String s_data = readSet.get(TPCC_NEW_ORDER_6+i).get(0);
    		ol_dist_info[i] = readSet.get(TPCC_NEW_ORDER_6+i).get(1);

    		if (i_data != null && s_data != null && (i_data.indexOf("original") != -1) && (s_data.indexOf("original") != -1)) {
            	bg[i] = 'B'; 
            } else {
            	bg[i] = 'G';
            }
        }
        
		//----------------------------------------------------------------
        pieces = new ArrayList<Piece>();
		pieces.add(TPCC_NEW_ORDER_3(o_id));
		pieces.add(TPCC_NEW_ORDER_4(o_id));
		pieces.addAll(TPCC_NEW_ORDER_8(w_tax, d_tax, o_id, c_discount, i_prices, ol_dist_info));
		
		startListener = new StartListener(pieces, this);
        members.addAll(startListener.start());

        synchronized (startListener) {
            long start = System.currentTimeMillis();
			while (!startListener.isFinished()) {
				try {
			        LOG.warn("neworder waiting 2");
					startListener.wait(2000);
				} catch (InterruptedException ignored) { }
				if (System.currentTimeMillis() - start > 10000) {
	                LOG.warn("Pieces timed out");
	                break;
	            }
			}
		}
        LOG.info("Commit: " + transactionId);
		
		Graph graph = new Graph();
		graph.setVertexes(dep);
		graph.setServersInvolved(serversInvolvedList);

		//----------------------------------------------------------------
		// send out asynchronized to all nodes to commit transaction
		CommitListener commitListener = new CommitListener(members, this, transactionId, graph);
		commitListener.start();
		
		// wait until all responses received and transaction done
		synchronized (commitListener) {
			long start = System.currentTimeMillis();
			while (commitListener.getCount() < members.size()) {
			    try {
			        LOG.debug("Transaction " + transactionId + " waiting for commit");
			        commitListener.wait(5000);
				} catch (InterruptedException ignored) { }

			    if (System.currentTimeMillis() - start > 20000) {
			        LOG.fatal("Transaction " + transactionId + " timed out");
			        break;
			    }
			}
		}
//		// now simply remove the transaction from the dep graph
//		dep.remove(transactionId);
//		LOG.debug("remove: " + transactionId);
		
		// TPCC_NEW_ORDER_7
		for (int i = 0; i < o_ol_cnt; i++) {
			try{
			s_quantities[i] = Integer.parseInt(readSet.get(TPCC_NEW_ORDER_7+i).get(0));
			} catch (Exception e) {
				LOG.debug("@@@ " + o_ol_cnt);
				LOG.debug("@@@ " + i + " " + readSet.get(TPCC_NEW_ORDER_7+i));
				throw e;
			}
		}

		boolean valid = true;
		LOG.debug("==============================New Order==================================");
		LOG.debug("Warehouse: " + w_id + "\tDistrict: " + d_id);
		if (valid) {
			LOG.debug("Customer: " + c_id + "\tName: " + c_last + "\tCredit: " + c_credit + "\tDiscount: " + c_discount);
			LOG.debug("Order Number: " + o_id + " OrderId: " + o_id + " Number_Lines: " + o_ol_cnt + " W_tax: " + w_tax + " D_tax: " + d_tax + "\n");
			LOG.debug("Supp_W Item_Id           Item Name     ol_q s_q  bg Price Amount");
			for (int i = 0; i < o_ol_cnt; i++) {
				LOG.debug( String.format("  %4d %6d %24s %2d %4d %3c %6.2f %6.2f",
						supware[i], ol_i_ids[i], i_names[i], ol_quantities[i], s_quantities[i], bg[i], i_prices[i], ol_amounts[i]));
			}
		} else {
			LOG.debug("Customer: " + c_id + "\tName: " + c_last + "\tCredit: " + c_credit + "\tOrderId: " + o_id);
			LOG.debug("Exection Status: Item number is not valid");
		}
		LOG.debug("=========================================================================");
		
		return true;
	}
	
	@Override
	public Map<Integer, List<String>> getReadSet() {
		return this.readSet;
	}

	@Override
	public Map<String, String> getGraph() {
		return this.dep;
	}

	@Override
	public Map<String, Set<String>> getServersInvolvedList() {
		return this.serversInvolvedList;
	}

	@Override
	public CoordinatorCommunicator getCommunicator() {
		return this.communicator;
	}
	
	public static void main(String[] args) {

	}
}
