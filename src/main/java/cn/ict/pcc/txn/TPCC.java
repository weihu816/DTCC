package cn.ict.pcc.txn;
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
import java.util.concurrent.atomic.AtomicLong;

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
	public static AtomicLong Latency = new AtomicLong(0);
	
	public static TransactionFactory transactionFactory = new TransactionFactory();
	
	
	public TPCC() {
		
	}

	public static void Neworder(int w_id, int d_id) throws TransactionException {
			
		PCCTransaction transaction = transactionFactory.create();
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
				if (s_quantity > ol_quantity - 10) {
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
	
	public static void main(String[] args) {

		PropertyConfigurator.configure(AppServerConfiguration.getConfiguration().getLogConfigFilePath());

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
				System.out.println("Thread" + i + ": " + x);
				result += x;
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Result: " + result);
		System.out.println("NumTxnsAborted: " + TPCC.numTxnsAborted);
		System.out.println("Commit Rate: " + (double) result / (result + numTxnsAborted.get()) * 100 + "%");
		System.out.println("Avg Latency: " + (double) Latency.get() / result + " ms");
		exec.shutdownNow();
	}
}

class myTask implements Callable<Integer> {

	@Override
	public Integer call() throws Exception {
		long start = System.currentTimeMillis();
		int count_neworder = 0;

		
//		while (System.currentTimeMillis() - start < 1) {
			try {
				long xxx = System.currentTimeMillis();
				int x = TPCCGenerator.randomInt(0,99);
				x = 1;
				if (x <= 47) {
					int w_id = TPCCGenerator.randomInt(1,TPCCConstants.NUM_WAREHOUSE);
					int d_id = TPCCGenerator.randomInt(1,TPCCConstants.DISTRICTS_PER_WAREHOUSE);
					TPCC.Neworder(w_id, d_id);
				} else if (x <= 93) {
					if (TPCCGenerator.randomInt(0,1) == 0) {
						
					} else {
						
					}
				} else {
					
				}
				TPCC.Latency.addAndGet(System.currentTimeMillis() - xxx);
				count_neworder++;
			} catch (TransactionException e) {
				TPCC.numTxnsAborted.addAndGet(1);
				throw e;
			}
			
//		}
		return count_neworder;
	}
}
