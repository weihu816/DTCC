package cn.ict.occ.txn;
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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import cn.ict.dtcc.benchmark.tpcc.TPCCConstants;
import cn.ict.dtcc.benchmark.tpcc.TPCCGenerator;
import cn.ict.dtcc.config.AppServerConfiguration;
import cn.ict.dtcc.exception.TransactionException;
import cn.ict.rcc.server.coordinator.messaging.RococoTransaction;

/**
 * TPCC Transaction Stored Procedures
 * @author Wei
 *
 */
public class TPCC {

	private static final Log LOG = LogFactory.getLog(TPCC.class);
	
	public static AtomicInteger numTxnsAborted = new AtomicInteger(0);
	
	static TransactionFactory transactionFactory = new TransactionFactory();
	
	
	public TPCC() {
		
	}

	public static void Neworder(int w_id, int d_id) throws TransactionException {
			
		OCCTransaction transaction = transactionFactory.create();
		transaction.begin();
		
		try {
			List<String> columns, values;
			int c_id = TPCCGenerator.NURand(TPCCConstants.A_C_ID, 1, TPCCConstants.CUSTOMERS_PER_DISTRICT);

			// Ri&R District : increase d_next_o_id
			String key_district = (TPCCGenerator.buildString(w_id, "_", d_id));
			columns = TPCCGenerator.buildColumns("d_next_o_id", "d_tax");

			values = transaction.read(TPCCConstants.TABLENAME_DISTRICT, key_district, columns);
			int o_id = Integer.parseInt(values.get(0));
			float d_tax = Float.parseFloat(values.get(1));
			LOG.debug("d_next_o_id = " + values.get(0));
			LOG.debug("d_tax = " + values.get(1));

			columns = TPCCGenerator.buildColumns("d_next_o_id");
			values = TPCCGenerator.buildColumns(o_id + 1);
			transaction.write(TPCCConstants.TABLENAME_DISTRICT, key_district, columns, values);

			// R warehouse
			// read w_tax, immediate ! READONLY
			String key_warehouse = String.valueOf(w_id);
			columns = TPCCGenerator.buildColumns("w_tax");
			values = transaction.read(TPCCConstants.TABLENAME_WAREHOUSE, key_warehouse, columns);

			float w_tax = Float.parseFloat(values.get(0));
			LOG.debug("w_tax = " + values.get(0));

			// R customer
			String key_customer = TPCCGenerator.buildString(w_id, "_", d_id, "_", c_id);
			columns = TPCCGenerator.buildColumns ("c_last", "c_credit", "c_discount");
			values = transaction.read(TPCCConstants.TABLENAME_CUSTOMER, key_customer, columns);

			String c_last = values.get(0);
			String c_credit = values.get(1);
			float c_discount = Float.valueOf(values.get(2));

			LOG.debug("c_discount: " + c_discount);
			LOG.debug("c_last: " + c_last);
			LOG.debug("c_credit: " + c_credit);

			//------------------------------------------------------------------
			int o_all_local = 1, o_ol_cnt = TPCCGenerator.randomInt(5, 15);
			int 	supware			[] = new int[o_ol_cnt];
			int 	ol_i_ids		[] = new int[o_ol_cnt];
			String i_names			[] = new String[o_ol_cnt];
			float i_prices			[] = new float[o_ol_cnt];
			float ol_amounts		[] = new float[o_ol_cnt];
			int ol_quantities		[] = new int[o_ol_cnt];
			int s_quantities		[] = new int[o_ol_cnt];
			char bg[] = new char[o_ol_cnt];
			for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
				int ol_supply_w_id; 
				/* 90% of supply are from home stock */
				int num_warehouse = TPCCConstants.NUM_WAREHOUSE;
				if (TPCCGenerator.randomInt(0, 99) < 10 &&  num_warehouse > 1) {
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

			// W order
			String key_order = TPCCGenerator.buildString(w_id, "_", d_id, "_", o_id);
			columns = TPCCGenerator.buildColumns("o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_id", "o_carrier_id", "o_ol_cnt", "o_all_local");
			values = TPCCGenerator.buildColumns(o_id, d_id, w_id, c_id, o_entry_d, "NULL", o_ol_cnt, o_all_local);
			transaction.write(TPCCConstants.TABLENAME_ORDER, key_order, columns, values);
			LOG.debug("W order");

			// W new_order
			String key_newOrder = key_order;
			columns = TPCCGenerator.buildColumns("no_w_id", "no_d_id", "no_o_id");
			values = TPCCGenerator.buildColumns(w_id, d_id, o_id);
			transaction.write(TPCCConstants.TABLENAME_NEW_ORDER, key_newOrder, columns, values);
			LOG.debug("W new_order");

			/* for each order in the order line*/
			for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
				//------------------------------------------------------------------
				int ol_supply_w_id 	= supware[ol_number - 1]; 
				int ol_i_id 		= ol_i_ids[ol_number - 1]; 
				int ol_quantity 	= TPCCGenerator.randomInt(1, 10);
				//------------------------------------------------------------------
				// Ri item
				String key_item = String.valueOf(ol_i_id);
				columns = TPCCGenerator.buildColumns("i_price", "i_name", "i_data");
				values = transaction.read(TPCCConstants.TABLENAME_ITEM, key_item, columns);
				float i_price = Float.valueOf(values.get(0));
				String i_name = values.get(1);
				String i_data = values.get(2);
				LOG.debug("Ri item | orderline#: " + ol_number);
				LOG.debug("i_name: " + i_name);
				LOG.debug("i_price: " + i_price);
				LOG.debug("i_data: " + i_data);


				// Ri stock, with probability to get conflict

				/* update stock quantity */
				String key_stock = TPCCGenerator.buildString(ol_supply_w_id, "_", ol_i_id);
				columns = TPCCGenerator.buildColumns("s_quantity", "s_data");
				columns.add("s_dist_" +  d_id);
				values = transaction.read(TPCCConstants.TABLENAME_STOCK, key_stock, columns);

				int s_quantity = Integer.valueOf(values.get(0));
				String s_data = values.get(1);
				String ol_dist_info = values.get(2);
				LOG.debug("Ri stock " + key_stock + "| orderline#: " + ol_number);
				LOG.debug("ol_dist_info: " + ol_dist_info);
				LOG.debug("s_data: " + s_data);
				LOG.debug("s_quantity: " + s_quantity);

				if ( i_data != null && s_data != null && (i_data.indexOf("original") != -1) && (s_data.indexOf("original") != -1) ) {
					bg[ol_number-1] = 'B'; 
				} else {
					bg[ol_number-1] = 'G';
				}

				// W stock
				if (s_quantity > ol_quantity) {
					s_quantity = s_quantity - ol_quantity;
				} else {
					s_quantity = s_quantity - ol_quantity + 91;
				}
				columns = TPCCGenerator.buildColumns("s_quantity");
				values = TPCCGenerator.buildColumns(s_quantity);
				transaction.write(TPCCConstants.TABLENAME_STOCK, key_stock, columns, values);
				LOG.debug("W stock | orderline#: " + ol_number);

				// W order_line			
				float ol_amount = ol_quantity * i_price *(1+w_tax+d_tax) *(1-c_discount); 
				String key_orderline = TPCCGenerator.buildString(w_id, "_", d_id , "_" , o_id , "_", ol_number);
				columns = TPCCGenerator.buildColumns("ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d",
						"ol_quantity", "ol_amount", "ol_dist_info");
				values = TPCCGenerator.buildColumns(o_id, d_id, w_id, ol_number, ol_i_id,
						ol_supply_w_id, "NULL", ol_quantity, ol_amount, ol_dist_info);
				transaction.write(TPCCConstants.TABLENAME_ORDER_LINE, key_orderline, columns, values);
				LOG.debug("W order_line | orderline#: " + ol_number);

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
		} catch (TransactionException e) {
			throw e;
//			numTxnsAborted++;
//			LOG.warn("Fail to commit transaction " + transaction.transactionId);
		}
		
		
	}
	
	
	public static void PaymentById(int w_id, int d_id, int c_id) throws TransactionException {
		
		// begin
		OCCTransaction transaction = transactionFactory.create();
		transaction.begin();

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

		// Ri&W warehouse
		String key_warehouse = String.valueOf(w_id);
		columns = TPCCGenerator.buildColumns("w_ytd", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip");
		values = transaction.read(TPCCConstants.TABLENAME_WAREHOUSE, key_warehouse, columns);
		String w_name 		= values.get(1);
		String w_street_1	= values.get(2);
		String w_street_2 	= values.get(3);
		String w_city 		= values.get(4);
		String w_state 		= values.get(5);
		String w_zip 		= values.get(6);

		float w_ytd = Float.parseFloat(values.get(0));
		w_ytd += h_amount;
		columns = TPCCGenerator.buildColumns("w_ytd");
		values = TPCCGenerator.buildColumns(w_ytd);
		transaction.write(TPCCConstants.TABLENAME_WAREHOUSE, key_warehouse, columns, values);
		LOG.debug("Ri&R Warehouse====================");
		LOG.debug("w_ytd = " + w_ytd);
		LOG.debug("w_name = " + w_name);
		LOG.debug("w_street_1 = " + w_street_1);
		LOG.debug("w_street_2 = " + w_street_2);
		LOG.debug("w_city = " + w_city);
		LOG.debug("w_state = " + w_state);
		LOG.debug("w_zip = " + w_zip);

		
		// Ri district
		String key_district = TPCCGenerator.buildString(w_id, "_", d_id);
		columns = TPCCGenerator.buildColumns("d_ytd", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip");
		values = transaction.read(TPCCConstants.TABLENAME_DISTRICT, key_district, columns);
		String d_name 		= values.get(1);
		String d_street_1 	= values.get(2);
		String d_street_2 	= values.get(3);
		String d_city 		= values.get(4);
		String d_state 		= values.get(5);
		String d_zip 		= values.get(6);
		float d_ytd = Float.parseFloat(values.get(0));
		d_ytd += h_amount;
		columns = TPCCGenerator.buildColumns("d_ytd");
		values = TPCCGenerator.buildColumns(d_ytd);
		transaction.write(TPCCConstants.TABLENAME_DISTRICT, key_district, columns, values);
		
		LOG.debug("Piece1: Ri district====================");
		LOG.debug("d_ytd = " + d_ytd);
		LOG.debug("d_name = " + d_name);
		LOG.debug("d_street_1 = " + d_street_1);
		LOG.debug("d_street_2 = " + d_street_2);
		LOG.debug("d_city = " + d_city);
		LOG.debug("d_state = " + d_state);
		LOG.debug("d_zip = " + d_zip);
		
		
		// piece 3, R customer secondary index, c_last -> c_id
		float c_balance = 0.0f;
		String c_data = null, h_data = null, c_first = null, c_middle = null, c_last = null;
		String c_street_1 = null, c_street_2 = null, c_city = null, c_state = null, c_zip = null;
		String c_phone = null, c_credit = null, c_credit_lim = null, c_since = null;

		// get customre by c_id
		String key_customer = TPCCGenerator.buildString(c_w_id, "_", c_d_id, "_", c_id);
		columns = TPCCGenerator.buildColumns("c_balance", "c_data", "c_first",
				"c_middle", "c_last", "c_street_1", "c_street_2", "c_city",
				"c_state", "c_zip", "c_phone", "c_credit", "c_credit_lim", "c_since");
		values = transaction.read(TPCCConstants.TABLENAME_CUSTOMER, key_customer, columns);
		
		c_balance = Float.valueOf(values.get(0));
		c_data = values.get(1);
		c_first = values.get(2);
		c_middle = values.get(3);
		c_last = values.get(4);
		c_street_1 = values.get(5);
		c_street_2 = values.get(6);
		c_city = values.get(7);
		c_state = values.get(8);
		c_zip = values.get(9);
		c_phone = values.get(10);
		c_credit = values.get(11);
		c_credit_lim = values.get(12);
		c_since = values.get(13);
		
		
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
		} else {
			/* update customer c_balance */
			columns = TPCCGenerator.buildColumns("c_balance");
			values = TPCCGenerator.buildColumns(c_balance);
			transaction.write(TPCCConstants.TABLENAME_CUSTOMER, key_customer, columns, values);
		}
		
		// insert history
		String key_history = String.valueOf(System.currentTimeMillis());
		/* insert into history table */
		columns = TPCCGenerator.buildColumns("h_c_d_id", "h_c_w_id", "h_c_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data");
		values = TPCCGenerator.buildColumns(c_d_id, c_w_id, c_id, d_id, w_id, h_date, h_amount, h_data);
		transaction.write(TPCCConstants.TABLENAME_HISTORY, key_history, columns, values);
		
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
		LOG.debug( "Cust-data:\t" + c_data.substring(0, n));
		for (int i = 1; i < num_line - 1; i++) {
			LOG.debug("\t\t" + c_data.substring(n*i, n*(i+1)));
		}
		LOG.debug("\t\t" + c_data.substring(n*(num_line-1)));
		LOG.debug("=========================================================================");
	}

	
	public static void PaymentByLastname(int w_id, int d_id, String c_last) throws TransactionException {
		
		// begin
		OCCTransaction transaction = transactionFactory.create();
		transaction.begin();

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

		// Ri&W warehouse
		String key_warehouse = String.valueOf(w_id);
		columns = TPCCGenerator.buildColumns("w_ytd", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip");
		values = transaction.read(TPCCConstants.TABLENAME_WAREHOUSE, key_warehouse, columns);
		String w_name 		= values.get(1);
		String w_street_1	= values.get(2);
		String w_street_2 	= values.get(3);
		String w_city 		= values.get(4);
		String w_state 		= values.get(5);
		String w_zip 		= values.get(6);

		float w_ytd = Float.parseFloat(values.get(0));
		w_ytd += h_amount;
		columns = TPCCGenerator.buildColumns("w_ytd");
		values = TPCCGenerator.buildColumns(w_ytd);
		transaction.write(TPCCConstants.TABLENAME_WAREHOUSE, key_warehouse, columns, values);
		LOG.debug("Ri&R Warehouse====================");
		LOG.debug("w_ytd = " + w_ytd);
		LOG.debug("w_name = " + w_name);
		LOG.debug("w_street_1 = " + w_street_1);
		LOG.debug("w_street_2 = " + w_street_2);
		LOG.debug("w_city = " + w_city);
		LOG.debug("w_state = " + w_state);
		LOG.debug("w_zip = " + w_zip);

		
		// RW district
		String key_district = TPCCGenerator.buildString(w_id, "_", d_id);
		columns = TPCCGenerator.buildColumns("d_ytd", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip");
		values = transaction.read(TPCCConstants.TABLENAME_DISTRICT, key_district, columns);
		String d_name 		= values.get(1);
		String d_street_1 	= values.get(2);
		String d_street_2 	= values.get(3);
		String d_city 		= values.get(4);
		String d_state 		= values.get(5);
		String d_zip 		= values.get(6);
		float d_ytd = Float.parseFloat(values.get(0));
		d_ytd += h_amount;
		columns = TPCCGenerator.buildColumns("d_ytd");
		values = TPCCGenerator.buildColumns(d_ytd);
		transaction.write(TPCCConstants.TABLENAME_DISTRICT, key_district, columns, values);
		
		LOG.debug("Piece1: Ri district====================");
		LOG.debug("d_ytd = " + d_ytd);
		LOG.debug("d_name = " + d_name);
		LOG.debug("d_street_1 = " + d_street_1);
		LOG.debug("d_street_2 = " + d_street_2);
		LOG.debug("d_city = " + d_city);
		LOG.debug("d_state = " + d_state);
		LOG.debug("d_zip = " + d_zip);
		
		
		// piece 3, R customer secondary index, c_last -> c_id
		float c_balance = 0.0f;
		String c_data = null, h_data = null, c_first = null, c_middle = null;
		String c_street_1 = null, c_street_2 = null, c_city = null, c_state = null, c_zip = null;
		String c_phone = null, c_credit = null, c_credit_lim = null, c_since = null;

		// get customre by c_id
		String key_customer_index = TPCCGenerator.buildString(c_w_id, "_", c_d_id, "_", c_last);
		columns = TPCCGenerator.buildColumns("c_id", "c_balance", "c_data", "c_first",
				"c_middle", "c_last", "c_street_1", "c_street_2", "c_city",
				"c_state", "c_zip", "c_phone", "c_credit", "c_credit_lim", "c_since");
		values = transaction.readIndexFetchMiddle(TPCCConstants.TABLENAME_CUSTOMER, key_customer_index, columns, "c_first", true);
		
		int c_id 	 = Integer.parseInt(values.get(0)); // position 0 is the key
		c_balance 	 = Float.valueOf(values.get(1));
		c_data 		 = values.get(2);
		c_first 	 = values.get(3);
		c_middle 	 = values.get(4);
		c_last 		 = values.get(5);
		c_street_1 	 = values.get(6);
		c_street_2 	 = values.get(7);
		c_city 		 = values.get(8);
		c_state 	 = values.get(9);
		c_zip 		 = values.get(10);
		c_phone 	 = values.get(11);
		c_credit 	 = values.get(12);
		c_credit_lim = values.get(13);
		c_since		 = values.get(14);
		String key_customer = TPCCGenerator.buildString(c_w_id, "_", c_d_id, "_", c_id);

		
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
		} else {
			/* update customer c_balance */
			columns = TPCCGenerator.buildColumns("c_balance");
			values = TPCCGenerator.buildColumns(c_balance);
			transaction.write(TPCCConstants.TABLENAME_CUSTOMER, key_customer, columns, values);
		}
		
		// insert history
		String key_history = String.valueOf(System.currentTimeMillis());
		/* insert into history table */
		columns = TPCCGenerator.buildColumns("h_c_d_id", "h_c_w_id", "h_c_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data");
		values = TPCCGenerator.buildColumns(c_d_id, c_w_id, c_id, d_id, w_id, h_date, h_amount, h_data);
		transaction.write(TPCCConstants.TABLENAME_HISTORY, key_history, columns, values);
		
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
		OCCTransaction transaction = transactionFactory.create();
		transaction.begin();
		
		int w_id = TPCCGenerator.randomInt(1, TPCCConstants.NUM_WAREHOUSE);
		int d_id = TPCCGenerator.randomInt(1, TPCCConstants.DISTRICTS_PER_WAREHOUSE);
//		w_id = 1;
//		d_id = 8;
		
		List<String> columns, values;
		/* ORDER BY no_o_id ASC and choose an new order */
		String key_neworder_secondary = TPCCGenerator.buildString(w_id, "_", d_id);		
		columns = TPCCGenerator.buildColumns("no_o_id");

		// get neworder id
		List<String> names = TPCCGenerator.buildColumns("no_o_id");
		values = transaction.readIndexFetchTop(TPCCConstants.TABLENAME_NEW_ORDER, key_neworder_secondary, names, "", true);
		
		/* If no matching row is found, then the delivery of an order for this district is skipped. */
		int no_o_id = Integer.valueOf(values.get(0));
		LOG.debug("Fetch no_o_id");
		LOG.debug("no_o_id: " + no_o_id);

		// delete new_order
		String key_neworder = TPCCGenerator.buildString(w_id, "_", d_id, "_", no_o_id);
		columns = TPCCGenerator.buildColumns("new_order");
		transaction.delete(TPCCConstants.TABLENAME_NEW_ORDER, key_neworder);;
		LOG.info("Delete from new_order Key:" + key_neworder);
		
		// Ri & W order
		// get the customer id for this order
		String key_order = TPCCGenerator.buildString(w_id, "_", d_id, "_", no_o_id);
		names = TPCCGenerator.buildColumns("o_c_id", "o_carrier_id");
		values = transaction.read(TPCCConstants.TABLENAME_ORDER, key_order, names);
		int o_c_id = Integer.valueOf(values.get(0));

		columns = TPCCGenerator.buildColumns("o_carrier_id");
		names = TPCCGenerator.buildColumns(o_carrier_id);
		transaction.write(TPCCConstants.TABLENAME_ORDER, key_order, columns, names);
		LOG.debug("RW order");
		
		// piece: Ri & W order_line
		String ol_delivery_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		float ol_total = 0.0f;
		String key_prefix_orderline = TPCCGenerator.buildString(w_id, "_", d_id, "_", no_o_id);
		columns = TPCCGenerator.buildColumns("ol_number", "ol_amount");
		List<List<String>> lists = transaction.readIndexFetchAll(
				TPCCConstants.TABLENAME_ORDER_LINE, key_prefix_orderline, columns);
		
		for (List<String> list : lists) {
			String ol_number = list.get(0);
			ol_total += Float.valueOf(list.get(1)); 
			String key_orderline = TPCCGenerator.buildString(w_id, "_", d_id, "_", no_o_id, "_", ol_number);
			List<String> columns_neworder = TPCCGenerator.buildColumns("ol_delivery_d");
			List<String> values_neworder = TPCCGenerator.buildColumns(ol_delivery_d);
			transaction.write(TPCCConstants.TABLENAME_ORDER_LINE, key_orderline, columns_neworder, values_neworder);
			LOG.debug("write orderline: " + key_orderline);
		}
		
		//---------------------------------------------------------------------------
		// W customer
		String key_customer = TPCCGenerator.buildString(w_id, "_", d_id, "_", o_c_id);
		columns = TPCCGenerator.buildColumns("c_balance", "c_delivery_cnt");
		values = transaction.read(TPCCConstants.TABLENAME_CUSTOMER, key_customer, columns);
		
		float c_balance = Float.valueOf(values.get(0));
		int c_delivery_cnt = Integer.valueOf(values.get(1));
		LOG.debug("write customer");
		LOG.debug("c_balance: " + c_balance);
		LOG.debug("c_delivery_cnt: " + c_delivery_cnt);

		/* update c_balance, c_delivery_cnt of customers */
		columns = TPCCGenerator.buildColumns("c_balance", "c_delivery_cnt");
		values = TPCCGenerator.buildColumns(c_balance + ol_total, c_delivery_cnt + 1);
		transaction.write(TPCCConstants.TABLENAME_CUSTOMER, key_customer, columns, values);
		
		transaction.commit();
		LOG.debug("==============================Delivery==================================");
		LOG.debug("INPUT	o_carrier_id: " + o_carrier_id);
		LOG.debug("Warehouse: " + w_id);
		LOG.debug("o_carrier_id: " + o_carrier_id);
		LOG.debug("Execution Status: Delivery has been queued");
		LOG.debug("=========================================================================");
	}
	public static void main(String[] args) {

		PropertyConfigurator.configure(AppServerConfiguration.getConfiguration().getLogConfigFilePath());

		int n = 4;
		ExecutorService exec = Executors.newFixedThreadPool(n);
		Future<Integer>[] futures = new Future[n];
		int result = 0;
		for (int i = 0; i < n; i++) {
			futures[i] = exec.submit(new myTask());
		}
		
		for (int i = 0; i < n; i++) {
			try {
				int x = futures[i].get();
				System.out.println("Thread" + i + ": " + x);
				result += x;
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Result: " + result);
		System.out.println("NumTxnsAborted: " + TPCC.numTxnsAborted);
		System.out.println("Commit Rate: " + (double) result / (result + numTxnsAborted.get()) * 100 + "%");
		exec.shutdownNow();
	}
}

class myTask implements Callable<Integer> {

	@Override
	public Integer call() throws Exception {
		

		long start = System.currentTimeMillis();
		int count_neworder = 0;

		
		while (System.currentTimeMillis() - start < 10000) {
			try {
				int x = TPCCGenerator.randomInt(0,99);
				x = 1;
				if (x <= 50) {
					int w_id = TPCCGenerator.randomInt(1,TPCCConstants.NUM_WAREHOUSE);
					int d_id = TPCCGenerator.randomInt(1,TPCCConstants.DISTRICTS_PER_WAREHOUSE);
					TPCC.Neworder(w_id, d_id);
					count_neworder++;
				} else if (x <= 90) {
					if (TPCCGenerator.randomInt(0,1) == 0) {
						int w_id = TPCCGenerator.randomInt(1,TPCCConstants.NUM_WAREHOUSE);
						int d_id = TPCCGenerator.randomInt(1,TPCCConstants.DISTRICTS_PER_WAREHOUSE);
						int c_id = TPCCGenerator.randomInt(1,  TPCCConstants.CUSTOMERS_PER_DISTRICT);
						TPCC.PaymentById(w_id, d_id, c_id);
					} else {
						int w_id = TPCCGenerator.randomInt(1,TPCCConstants.NUM_WAREHOUSE);
						int d_id = TPCCGenerator.randomInt(1,TPCCConstants.DISTRICTS_PER_WAREHOUSE);
						String c_last = TPCCGenerator.Lastname(TPCCGenerator.NURand(TPCCConstants.A_C_LAST, 0, TPCCConstants.CUSTOMER_LASTNAME_BOUND - 1));
						TPCC.PaymentByLastname(w_id, d_id, c_last);
					}

				} else {
					int o_carrier_id = TPCCGenerator.randomInt(1,10);
					TPCC.Delivery(o_carrier_id);
				}
			} catch (TransactionException e) {
				TPCC.numTxnsAborted.addAndGet(1);
			}
			
//			try {
//	            System.out.println("Starting another txn in 1 seconds...");
//	            Thread.sleep(10);
//	        } catch (InterruptedException ignored) {
//	        }
		}
		return count_neworder;
	}
}
