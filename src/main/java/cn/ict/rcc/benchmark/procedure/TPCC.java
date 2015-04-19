package cn.ict.rcc.benchmark.procedure;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import cn.ict.dtcc.benchmark.tpcc.TPCCConstants;
import cn.ict.dtcc.benchmark.tpcc.TPCCGenerator;
import cn.ict.dtcc.exception.TransactionException;
import cn.ict.rcc.server.coordinator.messaging.CoordinatorClient;
import cn.ict.rcc.server.coordinator.messaging.CoordinatorClientConfiguration;
import cn.ict.rcc.server.coordinator.messaging.RococoTransaction;
import cn.ict.rcc.server.coordinator.messaging.TransactionFactory;

/**
 * TPCC Transaction Stored Procedures
 * @author Wei
 *
 */
public class TPCC {

	private static final Log LOG = LogFactory.getLog(TPCC.class);
		
	static TransactionFactory transactionFactory = new TransactionFactory();
	
	
	public TPCC() {
		
	}

	public static void Neworder(int w_id, int d_id) throws TransactionException {
				
		// begin
		RococoTransaction transaction = transactionFactory.create();
		transaction.begin();
		
		List<String> columns, values;
		int c_id = TPCCGenerator.NURand(TPCCConstants.A_C_ID, 1, TPCCConstants.CUSTOMERS_PER_DISTRICT);
		
		// PIECE 1 Ri&R District
		// increase d_next_o_id, immediate
		String key_district = (TPCCGenerator.buildString(w_id, "_", d_id));
		int pieceNum_district = transaction.createPiece(TPCCConstants.TABLENAME_DISTRICT, key_district, true);
		columns = TPCCGenerator.buildColumns("d_next_o_id", "d_tax");
		transaction.readSelect(columns);
		transaction.addvalueInteger("d_next_o_id", 1);
		transaction.completePiece();
		int o_id = Integer.valueOf(transaction.get(pieceNum_district, "d_next_o_id"));
		float d_tax = Float.valueOf(transaction.get(pieceNum_district, "d_tax"));
		LOG.debug("Piece1:Ri&R District");
		LOG.debug("d_next_o_id = " 	+ o_id);
		LOG.debug("d_tax = " 		+ d_tax);

		// PIECE 2 R warehouse
		// read w_tax, immediate ! READONLY
		String key_warehouse = String.valueOf(w_id);
		columns = TPCCGenerator.buildColumns("w_tax");
		int pieceNum_warehouse = transaction.createPiece(TPCCConstants.TABLENAME_WAREHOUSE, key_warehouse, true);
		transaction.readSelect(columns);
		transaction.completePiece();
		float w_tax = Float.valueOf(transaction.get(pieceNum_warehouse, "w_tax"));
		LOG.debug("Piece 2: R warehouse");
		LOG.debug("w_tax: " + w_tax);
		
		// PIECE 3 R customer
		String key_customer = TPCCGenerator.buildString(w_id, "_", d_id, "_", c_id);
		columns = TPCCGenerator.buildColumns ("c_last", "c_discount", "c_credit");
		int pieceNum_customer = transaction.createPiece(TPCCConstants.TABLENAME_CUSTOMER, key_customer, true);
		transaction.readSelect(columns);
		transaction.completePiece();
		float c_discount 	= Float.valueOf(transaction.get(pieceNum_customer, "c_discount"));
		String c_last 		= transaction.get(pieceNum_customer, "c_last");
		String c_credit 	= transaction.get(pieceNum_customer, "c_credit");
		LOG.debug("Piece 3: R customer");
		LOG.debug("c_discount: " 	+ c_discount);
		LOG.debug("c_last: " 		+ c_last);
		LOG.debug("c_credit: " 		+ c_credit);
		//------------------------------------------------------------------
		int o_all_local = 1, o_ol_cnt = TPCCGenerator.randomInt(5, 15);
		int 	supware			[] 	= new int	[o_ol_cnt];
		int 	ol_i_ids		[] 	= new int	[o_ol_cnt];
		String i_names			[] 	= new String[o_ol_cnt];
		float i_prices			[] = new float[o_ol_cnt];
		float ol_amounts		[] = new float[o_ol_cnt];
		int ol_quantities		[] = new int[o_ol_cnt];
		int s_quantities		[] = new int[o_ol_cnt];
		char bg[] = new char[o_ol_cnt];
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
			int ol_supply_w_id; 
			/* 90% of supply are from home stock */
			if (TPCCGenerator.randomInt(0, 99) < 10 && TPCCConstants.NUM_WAREHOUSE > 1) {
				int supply_w_id = TPCCGenerator.randomInt(1, TPCCConstants.NUM_WAREHOUSE); 
				while (supply_w_id == w_id) { supply_w_id = TPCCGenerator.randomInt(1, TPCCConstants.NUM_WAREHOUSE); }
				ol_supply_w_id = supply_w_id;
			} else { ol_supply_w_id = w_id; }
			if (ol_supply_w_id != w_id) { o_all_local = 0; }
			supware[ol_number - 1] 	= ol_supply_w_id;
			ol_i_ids[ol_number - 1] = TPCCGenerator.NURand(TPCCConstants.A_OL_I_ID,1, TPCCConstants.NUM_ITEMS);
		}
		String o_entry_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		//------------------------------------------------------------------
		
		
		// PIECE 4 W order
		String key_order = TPCCGenerator.buildString(w_id, "_", d_id, "_", o_id);
		transaction.createPiece(TPCCConstants.TABLENAME_ORDER, key_order, false);
		columns = TPCCGenerator.buildColumns("o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_id", "o_carrier_id", "o_ol_cnt", "o_all_local");
		values = TPCCGenerator.buildColumns((o_id), (d_id), (w_id), (c_id), (o_entry_d), "NULL", (o_ol_cnt), (o_all_local));
		transaction.write(columns, values);
		transaction.completePiece();
		LOG.debug("Piece 4: W order");
		
		// PIECE 5 W new_order
		String key_newOrder = key_order;
		transaction.createPiece(TPCCConstants.TABLENAME_NEW_ORDER, key_newOrder, false);
		columns = TPCCGenerator.buildColumns("no_o_id", "no_d_id", "no_w_id");
		values = TPCCGenerator.buildColumns(o_id, d_id, w_id);
		transaction.write(columns, values);
		transaction.completePiece();
		// TODO :index
		LOG.debug("Piece 5: W new_order");

		/* for each order in the order line*/
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
			//------------------------------------------------------------------
			int ol_supply_w_id 	= supware	[ol_number - 1]; 
			int ol_i_id 		= ol_i_ids	[ol_number - 1]; 
			int ol_quantity 	= TPCCGenerator.randomInt(1, 10);
			//------------------------------------------------------------------
			// Piece 6 Ri item, conflict???????????? TODO
			String key_item = String.valueOf(ol_i_id);
			columns = TPCCGenerator.buildColumns("i_price", "i_name", "i_data");
			int pieceNum_item = transaction.createPiece(TPCCConstants.TABLENAME_ITEM, key_item, true);
			transaction.readSelect(columns);
			transaction.completePiece();
			String i_name = transaction.get(pieceNum_item, "i_name");
			float i_price = Float.valueOf(transaction.get(pieceNum_item, "i_price"));
			String i_data = transaction.get(pieceNum_item, "i_data");
			LOG.debug("Piece 6: Ri item | orderline#: " + ol_number);
			LOG.debug("i_name: " 	+ i_name);
			LOG.debug("i_price: " 	+ i_price);
			LOG.debug("i_data: " 	+ i_data);
			
			
			// Piece 7 Ri stock
			// 可能会有很小概率的冲突
			/* update stock quantity */
			String key_stock = TPCCGenerator.buildString(ol_supply_w_id, "_", ol_i_id);
			int pieceNum_stock = transaction.createPiece(TPCCConstants.TABLENAME_STOCK, key_stock, true);
			/* retrieve stock information */
			columns = TPCCGenerator.buildColumns("s_quantity", "s_data");
			columns.add("s_dist_" +  d_id);
			transaction.readSelect(columns);
			transaction.completePiece();
			String ol_dist_info = transaction.get(pieceNum_stock, "s_dist_" +  d_id);
			String s_data = transaction.get(pieceNum_stock, "s_data");
			int s_quantity = Integer.valueOf(transaction.get(pieceNum_stock, "s_quantity"));
			LOG.debug("Piece 7: Ri stock | orderline#: " + ol_number);
			LOG.debug("ol_dist_info: " + ol_dist_info);
			LOG.debug("s_data: " + s_data);
			LOG.debug("s_quantity: " + s_quantity);
			
			if ( i_data != null && s_data != null && (i_data.indexOf("original") != -1) && (s_data.indexOf("original") != -1) ) {
				bg[ol_number-1] = 'B'; 
			} else {
				bg[ol_number-1] = 'G';
			}
			// Piece 8  W stock
			pieceNum_stock = transaction.createPiece(TPCCConstants.TABLENAME_STOCK, key_stock, false);
			if (s_quantity > ol_quantity) {
				s_quantity = s_quantity - ol_quantity;
			} else {
				s_quantity = s_quantity - ol_quantity + 91;
			}
			transaction.write("s_quantity", String.valueOf(s_quantity));
			transaction.completePiece();
			LOG.debug("Piece 8: W stock | orderline#: " + ol_number);

			// Piece 9  W order_line			
			float ol_amount = ol_quantity * i_price *(1+w_tax+d_tax) *(1-c_discount); 
			String key_orderline = TPCCGenerator.buildString(w_id, "_", d_id , "_" , o_id , "_", ol_number);
			transaction.createPiece(TPCCConstants.TABLENAME_ORDER_LINE, key_orderline, false);
			columns = TPCCGenerator.buildColumns("ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_id",
					"ol_quantity", "ol_amount", "ol_dist_info");
			values = TPCCGenerator.buildColumns(o_id, d_id, w_id, ol_number, ol_i_id,
					ol_supply_w_id, "NULL", ol_quantity, ol_amount, ol_dist_info);
			transaction.write(columns, values);
			transaction.completePiece();
			LOG.debug("Piece 9: W order_line | orderline#: " + ol_number);

			i_names			[ol_number - 1] = i_name;
			i_prices		[ol_number - 1] = i_price;
			ol_amounts		[ol_number - 1] = ol_amount;
			ol_quantities	[ol_number - 1] = ol_quantity;
			s_quantities	[ol_number - 1] = s_quantity;
		}

		transaction.commit();
		
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
		
	}
	
	
	public static void Payment(int w_id, int d_id, String c_id_or_c_last) throws TransactionException {
		
		// begin
		RococoTransaction transaction = transactionFactory.create();
		transaction.begin();
		
		/* c_id or c_last */
		Boolean byname = false;

		float h_amount = TPCCGenerator.randomFloat(0, 5000);
		int x = TPCCGenerator.randomInt(1, 100);
		/*  the customer resident warehouse is the home 85% , remote 15% of the time  */
		int c_d_id, c_w_id;
		if (x <= 85) { 
			c_w_id = w_id;
			c_d_id = d_id;
		} else {
			c_d_id = TPCCGenerator.randomInt(1, TPCCConstants.DISTRICTS_PER_WAREHOUSE);
			do {
				c_w_id = TPCCGenerator.randomInt(1, TPCCConstants.NUM_WAREHOUSE);
			} while (c_w_id == w_id && TPCCConstants.NUM_WAREHOUSE > 1);
		}
		String h_date = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		List<String> columns, values;

		// piece 1 Ri&W warehouse
		String key_warehouse = String.valueOf(w_id);
		int pieceNum_warehouse = transaction.createPiece(TPCCConstants.TABLENAME_WAREHOUSE, key_warehouse, true);
		
		columns = TPCCGenerator.buildColumns("w_ytd", "w_name", "w_street_1",
				"w_street_2", "w_city", "w_state", "w_zip");
		
		transaction.readSelect(columns);
		transaction.addvalueDecimal("w_ytd", h_amount);
		transaction.completePiece();
		
		float  w_ytd 		= Float.valueOf(transaction.get(pieceNum_warehouse, "w_ytd"));
		String w_name 		= transaction.get(pieceNum_warehouse, "w_name");
		String w_street_1	= transaction.get(pieceNum_warehouse, "w_street_1");
		String w_street_2 	= transaction.get(pieceNum_warehouse, "w_street_2");
		String w_city 		= transaction.get(pieceNum_warehouse, "w_city");
		String w_state 		= transaction.get(pieceNum_warehouse, "w_state");
		String w_zip 		= transaction.get(pieceNum_warehouse, "w_zip");
		LOG.debug("Piece1: Ri&R Warehouse====================");
//		LOG.debug("w_ytd = " + w_ytd);
//		LOG.debug("w_name = " + w_name);
//		LOG.debug("w_street_1 = " + w_street_1);
//		LOG.debug("w_street_2 = " + w_street_2);
//		LOG.debug("w_city = " + w_city);
//		LOG.debug("w_state = " + w_state);
//		LOG.debug("w_zip = " + w_zip);
		
		// piece 2 Ri district & W district
		String key_district = TPCCGenerator.buildString(w_id, "_", d_id);
		columns = TPCCGenerator.buildColumns("d_name", "d_street_1",
				"d_street_2", "d_city", "d_state", "d_zip");
		int pieceNum_district = transaction.createPiece(TPCCConstants.TABLENAME_DISTRICT, key_district, true);
		transaction.readSelect(columns);
		transaction.addvalueDecimal("d_ytd", h_amount);
		transaction.completePiece();
		
//		float d_ytd 		= Float.valueOf(transaction.get(pieceNum_district, "d_ytd"));
		String d_name 		= (transaction.get(pieceNum_district, "d_name"));
		String d_street_1 	= (transaction.get(pieceNum_district, "d_street_1"));
		String d_street_2 	= (transaction.get(pieceNum_district, "d_street_2"));
		String d_city 		= (transaction.get(pieceNum_district, "d_city"));
		String d_state 		= (transaction.get(pieceNum_district, "d_state"));
		String d_zip 		= (transaction.get(pieceNum_district, "d_zip"));
		LOG.debug("Piece1: Ri district====================");
//		LOG.debug("d_ytd = " + d_ytd);
//		LOG.debug("d_name = " + d_name);
//		LOG.debug("d_street_1 = " + d_street_1);
//		LOG.debug("d_street_2 = " + d_street_2);
//		LOG.debug("d_city = " + d_city);
//		LOG.debug("d_state = " + d_state);
//		LOG.debug("d_zip = " + d_zip);
		
		// piece 3, R customer secondary index, c_last -> c_id
		float c_balance = 0.0f;
		String c_data = null, h_data = null, c_first = null, c_middle = null, c_last = null;
		String c_street_1 = null, c_street_2 = null, c_city = null, c_state = null, c_zip = null;
		String c_phone = null, c_credit = null, c_credit_lim = null, c_since = null;
		int c_id = 0;
		String key_customer = null, key_prefix_customer = null;
		if (byname) {
			key_prefix_customer = (TPCCGenerator.buildString(c_w_id + "_" + c_d_id));
			columns = TPCCGenerator.buildColumns("c_id", "c_balance", "c_credit", "c_data",
					"c_first", "c_middle", "c_last", "c_street_1",
					"c_street_2", "c_city", "c_state", "c_zip", "c_phone",
					"c_credit", "c_credit_lim", "c_since");
			int pieceNum_customer = transaction.createPiece(TPCCConstants.TABLENAME_CUSTOMER, key_customer, true);
			transaction.readSelect(columns);
			transaction.completePiece();
//			String constraintColumn = "c_last";
//			String ConstraintValue = ((String)c_id_or_c_last);
//			String orderColumn =  ("c_first");
//			results = db.read(CUSTOMER, key_prefix_customer, columns, constraintColumn, ConstraintValue, orderColumn, false);
//			int index = results.size() / 2;
//			result = results.get(index);
//			/* ORDER BY c_first and get midpoint */
//			c_id = Integer.valueOf((result.get(columns[0])));
//			c_balance = Float.valueOf((result.get(columns[1])));
//			c_credit = (result.get(columns[2]));
//			c_data = (result.get(columns[3]));
//			c_first = (result.get(columns[4]));
//			c_middle = (result.get(columns[5]));
//			c_last = (result.get(columns[6]));
//			c_street_1 = (result.get(columns[7]));
//			c_street_2 = (result.get(columns[8]));
//			c_city = (result.get(columns[9]));
//			c_state = (result.get(columns[10]));
//			c_zip = (result.get(columns[11]));
//			c_phone = (result.get(columns[12]));
//			c_credit = (result.get(columns[13]));
//			c_credit_lim = (result.get(columns[14]));
//			c_since = (result.get(columns[15]));
//			key_customer = buildString(c_w_id, "_", c_d_id, "_", c_id);
		} else {
			key_customer = TPCCGenerator.buildString(c_w_id, "_", c_d_id, "_", c_id_or_c_last);
			columns = TPCCGenerator.buildColumns("c_balance", "c_credit", "c_data",
					"c_first", "c_middle", "c_last", "c_street_1",
					"c_street_2", "c_city", "c_state", "c_zip", "c_phone",
					"c_credit", "c_credit_lim", "c_since");
			int pieceNum_customer = transaction.createPiece(TPCCConstants.TABLENAME_CUSTOMER, key_customer, true);
			transaction.readSelect(columns);
			transaction.completePiece();
			c_id 		 = Integer.valueOf(c_id_or_c_last);
			c_balance 	 = Float.valueOf(transaction.get(pieceNum_customer, "c_balance"));
			c_data 		 = transaction.get(pieceNum_customer, "c_data");
			c_first 	 = transaction.get(pieceNum_customer, "c_first");
			c_middle 	 = transaction.get(pieceNum_customer, "c_middle");
			c_last 		 = transaction.get(pieceNum_customer, "c_last");
			c_street_1 	 = transaction.get(pieceNum_customer, "c_street_1");
			c_street_2 	 = transaction.get(pieceNum_customer, "c_street_2");
			c_city 		 = transaction.get(pieceNum_customer, "c_city");
			c_state	 	 = transaction.get(pieceNum_customer, "c_state");
			c_zip 		 = transaction.get(pieceNum_customer, "c_zip");
			c_phone 	 = transaction.get(pieceNum_customer, "c_phone");
			c_credit 	 = transaction.get(pieceNum_customer, "c_credit");
			c_credit_lim = transaction.get(pieceNum_customer, "c_credit_lim");
			c_since 	 = transaction.get(pieceNum_customer, "c_since");
		}
		
		c_balance -= h_amount;
		h_data = TPCCGenerator.buildString(w_name, "    ", d_name);
		if (c_credit.equals("BC")) {
			String c_new_data = String.format("| %4d %2d %4d %2d %4d $%7.2f %12s %24s", 
					c_id,c_d_id, c_w_id, d_id, w_id, h_amount, h_date, h_data);
			c_new_data += c_data;
			if (c_new_data.length() > 500) {
				c_new_data = c_new_data.substring(0, 500);
			}
			/* update customer c_balance， c_data */
			columns = TPCCGenerator.buildColumns("c_balance", "c_data");
			values = TPCCGenerator.buildColumns(c_balance, c_new_data);
			transaction.createPiece(TPCCConstants.TABLENAME_CUSTOMER, key_customer, false);
			transaction.write(columns, values);
			transaction.completePiece();
			
		} else {
			/* update customer c_balance */
			columns = TPCCGenerator.buildColumns("c_balance");
			values = TPCCGenerator.buildColumns(c_balance);
			transaction.createPiece(TPCCConstants.TABLENAME_CUSTOMER, key_customer, false);
			transaction.write(columns, values);
			transaction.completePiece();
		}
		
		/* retrieve history key */
		String key_history = String.valueOf(System.currentTimeMillis());
		/* insert into history table */
		columns = TPCCGenerator.buildColumns("h_c_d_id", "h_c_w_id", "h_c_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data");
		values = TPCCGenerator.buildColumns(c_d_id, c_w_id, c_id, d_id, w_id, h_date, h_amount, h_data);
		transaction.createPiece(TPCCConstants.TABLENAME_HISTORY, key_history, false);
		transaction.write(columns, values);
		transaction.completePiece();
		
		transaction.commit();
	
		LOG.debug("==============================Payment====================================");
		LOG.debug("Date: " + h_date + " District: " + d_id);
		LOG.debug("Warehouse: " + w_id + "\t\t\t\tDistrict");
		LOG.debug(w_street_1 + "\t\t\t\t" + d_street_1);
		LOG.debug(w_street_2 + "\t\t\t\t" + d_street_2);
		LOG.debug(w_city + " " + w_state + " " + w_zip + "\t" + d_city + " " + d_state + " " + d_zip);
		LOG.debug("Customer: " + c_id + "\tCustomer-Warehouse: " + c_w_id + "\tCustomer-District: " + c_d_id);
		LOG.debug("Name:" + c_first + " " + c_middle + " " + c_last + "\tCust-Since:" + c_since);
		LOG.debug(c_street_1 + "\t\t\tCust-Credit:" + c_credit);
		LOG.debug(c_street_2);
		LOG.debug(c_city + " " + c_state + " " + c_zip + " \tCust-Phone:" + c_phone);
		LOG.debug("Amount Paid:" + h_amount  + "\t\t\tNew Cust-Balance: " + c_balance);
		LOG.debug("Credit Limit:" + c_credit_lim);
		if (c_credit.equals("BC")) {
			c_data = c_data.substring(0, 200);
		} 
		int length = c_data.length();
		int n = 50;
		int num_line = length / n;
		if (length % n != 0) num_line += 1;
		LOG.debug( "Cust-data: \t" + c_data.substring(0, n));
		for (int i = 1; i < num_line - 1; i++) {
			LOG.debug("\t\t" + c_data.substring(n*i, n*(i+1)));
		}
		LOG.debug("\t\t" + c_data.substring(n*(num_line-1)));
		LOG.debug("=========================================================================");
		
		
	}
	
	/*
	 * Function name: Delivery
	 * Description: The Delivery business transaction consists of processing a batch of 10 new (not yet delivered) orders.
	 * 				Each order is processed (delivered) in full within the scope of a read-write database transaction.
	 * Argument: o_carrier_id - randomly selected within [1 .. 10]
	 */
	public static void Delivery(int o_carrier_id) throws TransactionException {
		
		// begin
		RococoTransaction transaction = transactionFactory.create();
		transaction.begin();
		
		int w_id = TPCCGenerator.randomInt(1, TPCCConstants.NUM_WAREHOUSE);
		int d_id = TPCCGenerator.randomInt(1, TPCCConstants.DISTRICTS_PER_WAREHOUSE);
		List<String> columns, values;
		/* ORDER BY no_o_id ASC and choose an new order */
		int no_o_id = 0;
		String key_neworder_secondary = (TPCCGenerator.buildString(w_id, "_", d_id));		
		columns = TPCCGenerator.buildColumns("no_o_id");

		// piece 1, R customer 
		//secondary index, c_last -> c_id
		int pieceNum_neworder = transaction.createPiece(TPCCConstants.TABLENAME_NEW_ORDER, key_neworder_secondary, true);
		transaction.fetchOne(columns);
		transaction.completePiece();
		
		/* If no matching row is found, then the delivery of an order for this district is skipped. */
		no_o_id = Integer.valueOf(transaction.get(pieceNum_neworder, "no_o_id"));
		
		// delete
		String key_neworder = TPCCGenerator.buildString(w_id, "_", d_id, "_", no_o_id);
		columns = TPCCGenerator.buildColumns("new_order");
		transaction.createPiece(TPCCConstants.TABLENAME_NEW_ORDER, key_neworder, true);
		transaction.delete();
		transaction.completePiece();
		
		// Piece: Ri & W order
		// get the customer id for this order
		String key_order = TPCCGenerator.buildString(w_id, "_", d_id, "_", no_o_id);
		columns = TPCCGenerator.buildColumns("o_c_id");
		int pieceNum_order = transaction.createPiece(TPCCConstants.TABLENAME_ORDER, key_order, true);
		transaction.read("o_c_id");
		transaction.write("o_carrier_id", String.valueOf(o_carrier_id));
		transaction.completePiece();
		int o_c_id = Integer.valueOf(transaction.get(pieceNum_order, "o_c_id"));

		String key_customer = TPCCGenerator.buildString(w_id, "_", d_id, "_", o_c_id);
		
		String ol_delivery_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		float ol_total = 0.0f;
		String ol_number = null;
		String key_prefix_orderline = TPCCGenerator.buildString(w_id, "_", d_id, "_", no_o_id);
		columns = TPCCGenerator.buildColumns("ol_number", "ol_amount");
		
		// piece: Ri & W order_line
		int pieceNum_orderline = transaction.createPiece(TPCCConstants.TABLENAME_ORDER_LINE, key_prefix_orderline, true);
		transaction.fetchAll(columns);
		transaction.completePiece();
		
		List<Map<String, String>> results = transaction.getAll(pieceNum_orderline);
		for (Map<String, String> maps : results) {
			ol_number = (maps.get("ol_number"));
			ol_total += Float.valueOf(maps.get("ol_amount")); 
			String key_orderline = TPCCGenerator.buildString(w_id, "_", d_id, "_", no_o_id, "_", ol_number);

			List<String> columns_neworder = TPCCGenerator.buildColumns("ol_delivery_d");
			List<String> values_neworder = TPCCGenerator.buildColumns(ol_delivery_d);
			
			pieceNum_orderline = transaction.createPiece(TPCCConstants.TABLENAME_ORDER_LINE, key_orderline, false);
			transaction.write(columns_neworder, values_neworder);
			transaction.completePiece();
		}
		
		//---------------------------------------------------------------------------
		// W customer
		float c_balance =  0.0f;
		int c_delivery_cnt = 0;
		
		columns = TPCCGenerator.buildColumns("c_balance", "c_delivery_cnt");
		
		int pieceNum_customer = transaction.createPiece(TPCCConstants.TABLENAME_CUSTOMER, key_customer, true);
		transaction.readSelect(columns);
		transaction.completePiece();
		
		c_balance = Float.valueOf(transaction.get(pieceNum_customer, "c_balance"));
		c_delivery_cnt = Integer.valueOf(transaction.get(pieceNum_customer, "c_delivery_cnt"));
		
		/* update c_balance, c_delivery_cnt of customers */
		columns = TPCCGenerator.buildColumns("c_balance", "c_delivery_cnt");
		values = TPCCGenerator.buildColumns(c_balance + ol_total, c_delivery_cnt + 1);
		LOG.debug("write customer");
		
		transaction.createPiece(TPCCConstants.TABLENAME_CUSTOMER, key_customer, true);
		transaction.write(columns, values);
		transaction.completePiece();
		
		transaction.commit();
		LOG.debug("==============================Delivery==================================");
		LOG.debug("INPUT	o_carrier_id: " + o_carrier_id);
		LOG.debug("Warehouse: " + w_id);
		LOG.debug("o_carrier_id: " + o_carrier_id);
		LOG.debug("Execution Status: Delivery has been queued");
		LOG.debug("=========================================================================");
	}
	
	public static void main(String[] args) {

		PropertyConfigurator.configure(CoordinatorClientConfiguration
				.getConfiguration().getLogConfigFilePath());

		int n = 1;
		ExecutorService exec = Executors.newFixedThreadPool(n);
		Future<Integer>[] futures = new Future[n];
		int result = 0;
		for (int i = 0; i < n; i++) {
			futures[i] = exec.submit(new myTask());
		}
		
		for (int i = 0; i < n; i++) {
			try {
				int x = futures[i].get();
				System.out.println(x);
				result += x;
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		System.out.println(result);
		exec.shutdownNow();
	}
	
}

class myTask implements Callable<Integer> {

	@Override
	public Integer call() throws Exception {
		
		CoordinatorClient client = new CoordinatorClient();		

		long start = System.currentTimeMillis();
		int count_neworder = 0;

		List<String> paras;
		
//		while (System.currentTimeMillis() - start < 30000) {
//			int x = TPCCGenerator.randomInt(1, 100);
		int x = 1;
			if (x <= 44) {
				paras = new ArrayList<String>();
//				paras.add(String.valueOf(TPCCGenerator.randomInt(1,TPCCConstants.NUM_WAREHOUSE)));
//				paras.add(String.valueOf(TPCCGenerator.randomInt(1,TPCCConstants.DISTRICTS_PER_WAREHOUSE)));
				paras.add("1");
				paras.add("1");
				client.callProcedure(Procedure.TPCC_NEWORDER, paras);
				count_neworder++;
			} else if (x <= 87) {
				paras = new ArrayList<String>();
				paras.add(String.valueOf(TPCCGenerator.randomInt(1,TPCCConstants.NUM_WAREHOUSE)));
				paras.add(String.valueOf(TPCCGenerator.randomInt(1,TPCCConstants.DISTRICTS_PER_WAREHOUSE)));
				paras.add(String.valueOf(TPCCGenerator.randomInt(1,TPCCConstants.CUSTOMERS_PER_DISTRICT)));
				client.callProcedure(Procedure.TPCC_PAYMENT, paras);

			} else if (x <= 91) {

			} else if (x <= 95) {
				paras = new ArrayList<String>();
				paras.add(String.valueOf(TPCCGenerator.randomInt(1,10)));
				client.callProcedure(Procedure.TPCC_DELIVERY, paras);
			} else {
				
			}
//		}
		return count_neworder;
	}
}
