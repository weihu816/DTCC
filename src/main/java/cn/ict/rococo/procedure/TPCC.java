package cn.ict.rococo.procedure;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TPCC {

	private static final Log log = LogFactory.getLog(TPCC.class);
	
	static Random random = new Random();
	
	public static int COUNT_WARE = 1;
	/* The constants as specified */
	public static final int MAXITEMS 		= 100000;
	public static final int CUST_PER_DIST 	= 3000;
	public static final int DIST_PER_WARE	= 10;
	public static final int ORD_PER_DIST 	= 3000;
	/* NURand */
	public static final int A_C_LAST 	= 255;
	public static final int A_C_ID 		= 1023;
	public static final int A_OL_I_ID 	= 8191;
	public static final int C_C_LAST 	= randomInt(0, A_C_LAST);
	public static final int C_C_ID 		= randomInt(0, A_C_ID);
	public static final int C_OL_I_ID 	= randomInt(0, A_OL_I_ID);
	/* constant names of the column families */
	public static String WAREHOUSE 	= 	"warehouse";
	public static String DISTRICT 	= 	"district";
	public static String CUSTOMER 	= 	"customer";
	public static String HISTORY 	= 	"history";
	public static String ORDER 		= 	"order";
	public static String NEWORDER 	= 	"new_order";
	public static String ORDERLINE 	= 	"order_line";
	public static String ITEM 		= 	"item";
	public static String STOCK 		= 	"stock";
	
	boolean flag_output = false;
	
	static TransactionFactory transactionFactory = new TransactionFactory();
	
	
	public TPCC() {
		
	}

	/* 
	 * This function returns next random integer within the range 
	 */
	public static int randomInt(int min, int max) {
		return random.nextInt(max + 1) % (max - min + 1) + min;
	}
	
	/*
	 * This function returns next random float within the range
	 */
	public static float randomFloat(float min, float max) {
		return random.nextFloat() * (max - min) + min;
	}
	
	/*
	 * This function returns generated NR random number
	 */
	public static int NURand(int A, int x, int y) {
		int c = 0;
		switch (A) {
		case A_C_LAST:
			c = C_C_LAST;
			break;
		case A_C_ID:
			c = C_C_ID;
			break;
		case A_OL_I_ID:
			c = C_OL_I_ID;
			break;
		default:
		}
		return (((randomInt(0, A) | randomInt(x, y)) + c) % (y - x + 1)) + x;
	}
	
	/*
	 * The random name generation strategy
	 */
	public static String Lastname(int num) {
		String name = "";
		String n[] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
		name += n[num / 100];
		name += n[(num / 10) % 10];
		name += n[num % 10];
		return name;
	}
	
	public static String buildString(Object ... args) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof String) {
				stringBuffer.append((String)args[i]);
			} else {
				stringBuffer.append(String.valueOf(args[i]));
			}
			
		}
		return stringBuffer.toString();
	}
	
	public static List<String> buildColumns(Object ... args) {
		List<String> columns = new ArrayList<String>();
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof String) {
				columns.add((String)args[i]);
			} else {
				columns.add(String.valueOf(args[i]));
			}
		}
		return columns;
	}
	
	public static void Neworder(int w_id, int d_id) throws TransactionException {
		
		/* Begin transaction */
		RococoTransaction transaction = transactionFactory.create();
		transaction.begin();
		
		/* local variables */
		List<String> columns, values;
		int o_all_local = 1, c_id = NURand(A_C_ID, 1, CUST_PER_DIST), o_ol_cnt = randomInt(5, 15);
		String o_entry_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		
		// PIECE 1: IMMEDIATE+++++++++++++++++++++++++++++++++++++++++++++++++++
		/* New piece - increase d_next_o_id by one; Immediate Piece */
		String key_district = (buildString(w_id, "_", d_id));
		int pieceNum_district = transaction.createPiece(DISTRICT, key_district, true);
		columns = buildColumns("d_next_o_id", "d_tax");
		transaction.readSelect(columns);
		transaction.addvalue("d_next_o_id", 1);
		transaction.completePiece();
		// PIECE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		
		/* assign d_next_o_id to o_id */
		int o_id = Integer.parseInt(transaction.get(pieceNum_district, "d_next_o_id"));
		float d_tax = Float.valueOf(transaction.get(pieceNum_district, "d_tax"));
		log.info(o_id);
		log.info(d_tax);
		
		// PIECE 1: IMMEDIATE+++++++++++++++++++++++++++++++++++++++++++++++++++
		/* retrieve warehouse information  */
		String key_warehouse = String.valueOf(w_id);
		columns = buildColumns("w_tax");
		int pieceNum_warehouse = transaction.createPiece(WAREHOUSE, key_warehouse, true);
		transaction.readSelect(columns);
		transaction.completePiece();
		// PIECE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		float w_tax = Float.valueOf(transaction.get(pieceNum_warehouse, "w_tax"));
		log.info(w_tax);
		
//		// PIECE 1: IMMEDIATE+++++++++++++++++++++++++++++++++++++++++++++++++++
//		/* retrieve customer information */
//		String key_customer = buildString(w_id, "_", d_id, "_", c_id);
//		columns = buildColumns ("c_discount");
//		int pieceNum_customer = transaction.createPiece(CUSTOMER, key_customer, true);
//		transaction.readSelect(columns);
//		transaction.completePiece();
//		// PIECE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//
//		//------------------------------------------------------------------
//		int 	supware			[] 	= new int	[o_ol_cnt];
//		int 	ol_i_ids		[] 	= new int	[o_ol_cnt];
//		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
//			int ol_supply_w_id; 
//			/* 90% of supply are from home stock */
//			if (randomInt(0, 99) < 10 && COUNT_WARE > 1) {
//				int supply_w_id = randomInt(1, COUNT_WARE); 
//				while (supply_w_id == w_id) { supply_w_id = randomInt(1, COUNT_WARE); }
//				ol_supply_w_id = supply_w_id;
//			} else { ol_supply_w_id = w_id; }
//			if (ol_supply_w_id != w_id) { o_all_local = 0; }
//			supware[ol_number - 1] 	= ol_supply_w_id;
//			ol_i_ids[ol_number - 1] = NURand(A_OL_I_ID,1,100000);
//		}
//		//------------------------------------------------------------------
//		
//		
//		// PIECE 2 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//		/* insert into order table*/
//		String key_order = buildString(w_id, "_", d_id, "_", o_id);
//		transaction.createPiece(ORDER, key_order, false);
//		columns = buildColumns("o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_id", "o_carrier_id", "o_ol_cnt", "o_all_local");
//		values = buildColumns((o_id), (d_id), (w_id), (c_id), (o_entry_d), "NULL", (o_ol_cnt), (o_all_local));
//		transaction.write(columns, values);
//		transaction.completePiece();
//		// PIECE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//		
//		// PIECE 3 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//		/* insert into new order table */
//		String key_newOrder = key_order;
//		transaction.createPiece(NEWORDER, key_newOrder, false);
//		columns = buildColumns("no_o_id", "no_d_id", "no_w_id");
//		values = buildColumns(o_id, d_id, w_id);
//		transaction.write(columns, values);
//		transaction.completePiece();
//		// PIECE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//		
//		
//		/* for each order in the order line*/
//		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
//			//------------------------------------------------------------------
//			int ol_supply_w_id 	= supware[ol_number - 1]; 
//			int ol_i_id 		= ol_i_ids[ol_number - 1]; 
//			int ol_quantity 	= randomInt(1, 10);
//			//------------------------------------------------------------------
//			// PIECE read item IMMEDIATE+++++++++++++++++++++++++++++++++++++++++++++
//			/* retrieve item information */
//			String key_item = String.valueOf(ol_i_id);
//			columns = buildColumns("i_price");
//			int pieceNum_item = transaction.createPiece(STOCK, key_item, true);
//			transaction.readSelect(columns);
//			transaction.completePiece();
//			// PIECE ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//			
//			
//			// PIECE read stock IMMEDIATE++++++++++++++++++++++++++++++++++++++++++++
//			//可能会有很小概率的冲突
//			/* update stock quantity */
//			String key_stock = buildString(ol_supply_w_id, "_", ol_i_id);
//			int pieceNum_stock = transaction.createPiece(STOCK, key_stock, true);
//			/* retrieve stock information */
//			columns = buildColumns("s_quantity", "s_dist_01", "s_dist_02", "s_dist_03",
//					"s_dist_04", "s_dist_05", "s_dist_06", "s_dist_07", "s_dist_08",
//					"s_dist_09", "s_dist_10", "s_data");
//			transaction.readSelect(columns);
//			transaction.reducevalue("s_quantity", ol_quantity);
//			transaction.completePiece();
//			// PIECE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//						
//			// PIECE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//			String ol_dist_info = transaction.get(pieceNum_stock, "s_dist_" +  (d_id < 9 ? "0" + d_id : d_id));
//			float c_discount = Float.valueOf(transaction.get(pieceNum_customer, "c_discount"));
//			/* calculate order-line amount*/
//			float i_price = Float.valueOf(transaction.get(pieceNum_item, "i_price"));
//			float ol_amount = ol_quantity * i_price *(1+w_tax+d_tax) *(1-c_discount); 
//			/* insert into order line table */
//			String key_orderline = buildString(w_id, "_", d_id , "_" , o_id , "_", ol_number);
//			transaction.createPiece(ORDERLINE, key_orderline, false);
//			columns = buildColumns("ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_id",
//					"ol_quantity", "ol_amount", "ol_dist_info");
//			values = buildColumns(o_id, d_id, w_id, ol_number, ol_i_id,
//					ol_supply_w_id, "NULL", ol_quantity, ol_amount, ol_dist_info);
//			transaction.write(columns, values);
//			transaction.completePiece();
//			// PIECE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//			
//		}
		transaction.commit();
		
	}
	
}
