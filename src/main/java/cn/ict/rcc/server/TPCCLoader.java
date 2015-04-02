package cn.ict.rcc.server;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.StackKeyedObjectPool;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

import cn.ict.dtcc.benchmark.tpcc.TPCCConstants;
import cn.ict.dtcc.benchmark.tpcc.TPCCGenerator;
import cn.ict.dtcc.config.AppServerConfiguration;
import cn.ict.dtcc.config.Member;
import cn.ict.dtcc.config.ServerConfiguration;
import cn.ict.dtcc.messaging.ThriftConnectionPool;
import cn.ict.rcc.messaging.RococoCommunicationService;

/**
 * The loader function for initial state of tpcc benchmark
 * @author Wei Hu
 */
public class TPCCLoader {

	private static final Log LOG = LogFactory.getLog(TPCCLoader.class);
	private KeyedObjectPool<Member, TTransport> blockingPool = new 
			StackKeyedObjectPool<Member, TTransport>(new ThriftConnectionPool());
	private HashMap<Member, RococoCommunicationService.Client> clientPool = new HashMap<Member, RococoCommunicationService.Client>();
	private AppServerConfiguration config;
	
	
	public TPCCLoader() { 
		config = AppServerConfiguration.getConfiguration();
	}
	
	public void load() {
		List<String> fields_neworder = new ArrayList<String>();
		fields_neworder.add("no_w_id");
		fields_neworder.add("no_d_id");
		createSecondaryIndex(TPCCConstants.TABLENAME_NEW_ORDER, fields_neworder);
		List<String> fields_orderline = new ArrayList<String>();
		fields_orderline.add("ol_w_id");
		fields_orderline.add("ol_d_id");
		fields_orderline.add("ol_o_id");
		createSecondaryIndex(TPCCConstants.TABLENAME_ORDER_LINE, fields_orderline);
		LoadItems();
		LoadWare();
		LoadCust();
		LoadOrd();
	}

	private boolean write(String table, String key, List<String> names, List<String> values) {
		Member member = null;
		TTransport transport = null;
		try {
			member = config.getShardMember(table, key);
			RococoCommunicationService.Client client = clientPool.get(member);
			if (client == null) {
				transport = blockingPool.borrowObject(member);
				client = new RococoCommunicationService.Client(new TBinaryProtocol(transport));
				clientPool.put(member, client);
			}
			return client.write(table, key, names, values);
		} catch (Exception e) {
			if (member != null) {
				String msg = "Error contacting the remote member: " + member.getHostName() + member.getPort();
				LOG.warn(msg, e);
			}
			return false;
		}
	}
	
	private boolean createSecondaryIndex(String table, List<String> fields) {
		TTransport transport = null;
		try {
			for(Member member : config.getMembers()) {
				RococoCommunicationService.Client client = clientPool.get(member);
				if (client == null) {
					transport = blockingPool.borrowObject(member);
					client = new RococoCommunicationService.Client(new TBinaryProtocol(transport));
					clientPool.put(member, client);
				}
				client.createSecondaryIndex(table, fields);
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	/*
	 * This function Load items into item table
	 */
//	private class ItemLoader implements Callable<Integer> {
//		int start_id, end_id;
//		int[] orig;
//
//		private ItemLoader(int start_id, int end_id, int[] orig) {
//			this.start_id = start_id;
//			this.end_id = end_id;
//			this.orig = orig;
//		}
//
//		@Override
//		public Integer call() throws Exception {
//			String i_name, i_data;
//			float i_price;
//			int idatasiz, pos;
//			for (int i_id = start_id; i_id <= end_id; i_id++) {
//				i_name 	= TPCCGenerator.makeAlphaString(14, 24);
//				i_price = TPCCGenerator.randomFloat(100, 10000) / 100.0f;
//				i_data 	= TPCCGenerator.makeAlphaString(26, 50);
//				idatasiz = i_data.length();
//				if (orig[i_id - 1] == 1) {
//					pos = TPCCGenerator.randomInt(0, idatasiz - 8);
//					i_data = i_data.substring(0, pos) + "original" + i_data.substring(pos + 8);
//				}
//				String key = String.valueOf(i_id);
//				List<String> columns = TPCCGenerator.buildColumns("i_id", "i_name", "i_price", "i_data");
//				List<String> values = TPCCGenerator.buildColumns(i_id, i_name, i_price, i_data);
//				LOG.info("write: "  + TPCCConstants.TABLENAME_ITEM  + " "+ key);
//				write(TPCCConstants.TABLENAME_ITEM, key, columns, values);
//			}
//			LOG.info("Item Done. (" + start_id + "-" + end_id + ")");
//			return 0;
//		}
//	}

	public void LoadItems() {
		String i_name, i_data;
		float i_price;
		int idatasiz, pos = 0;
		int orig[] = new int[TPCCConstants.NUM_ITEMS];
		/* random of 10% items that will be marked 'original ' */
		for (int i = 0; i < TPCCConstants.NUM_ITEMS; i++) {
			orig[i] = 0;
		}
		for (int i = 0; i < TPCCConstants.NUM_ITEMS / 10; i++) {
			do {
				pos = (new Random()).nextInt(TPCCConstants.NUM_ITEMS);
			} while (orig[pos] == 1);
			orig[pos] = 1;
		}
		LOG.info("Loading Item");
//		int NUMBER_THREADS = TPCCConfig.LOADER_NUM_THREADS;
//		ExecutorService exec = Executors.newFixedThreadPool(NUMBER_THREADS);
//		Future<Integer>[] futures = new Future[NUMBER_THREADS];
//		int num = TPCCConstants.NUM_ITEMS / NUMBER_THREADS;
//		for (int i = 0; i < NUMBER_THREADS - 1; i++) {
//			futures[i] = exec.submit(new ItemLoader(num * i + 1, num * (i + 1), orig));
//		}
//		futures[NUMBER_THREADS - 1] = exec.submit(new ItemLoader(num*(NUMBER_THREADS - 1) + 1, TPCCConstants.NUM_ITEMS, orig));
//		for (int i = 0; i < NUMBER_THREADS; i++) {
//			try {
//				futures[i].get();
//			} catch (InterruptedException | ExecutionException e) {
//				e.printStackTrace();
//			}
//		}
//		exec.shutdownNow();
		for (int i_id = 1; i_id <= TPCCConstants.NUM_ITEMS; i_id++) {
			i_name 	= TPCCGenerator.makeAlphaString(14, 24);
			i_price = TPCCGenerator.randomFloat(100, 10000) / 100.0f;
			i_data 	= TPCCGenerator.makeAlphaString(26, 50);
			idatasiz = i_data.length();
			if (orig[i_id - 1] == 1) {
				pos = TPCCGenerator.randomInt(0, idatasiz - 8);
				i_data = i_data.substring(0, pos) + "original" + i_data.substring(pos + 8);
			}
			String key = String.valueOf(i_id);
			List<String> columns = TPCCGenerator.buildColumns("i_id", "i_name", "i_price", "i_data");
			List<String> values = TPCCGenerator.buildColumns(i_id, i_name, i_price, i_data);
//			LOG.info("write: "  + TPCCConstants.TABLENAME_ITEM  + " "+ key);
			write(TPCCConstants.TABLENAME_ITEM, key, columns, values);
		}
		LOG.info("Item Done.");
	}

	/*
	 * Function name: LoadWare Description: Load the stock table, then call
	 * Stock and District Argument: none
	 */
	public void LoadWare() {
		int w_id;
		String w_name, w_street_1, w_street_2, w_city, w_state, w_zip;
		float w_tax, w_ytd;

		/* start loading */
		LOG.info("Loading Warehouses");

		for (w_id = 1; w_id <= TPCCConstants.NUM_WAREHOUSE; w_id++) {
			/* Generate Warehouse Data */
			w_name 		= TPCCGenerator.makeAlphaString(6, 10);
			w_street_1 	= TPCCGenerator.makeAlphaString(10, 20); 	/* Street 1 */
			w_street_2 	= TPCCGenerator.makeAlphaString(10, 20); 	/* Street 2 */
			w_city 		= TPCCGenerator.makeAlphaString(10, 20); 	/* City */
			w_state 	= TPCCGenerator.makeAlphaString(2, 2); 		/* State */
			w_zip 		= TPCCGenerator.makeNumberString(9, 9); 	/* Zip */
			w_tax 		= TPCCGenerator.randomFloat(10, 20) / 100.0f;
			w_ytd 		= 3000000.0f;

			/* key */
			String key = String.valueOf(w_id);
			/* column */
			List<String> columns = TPCCGenerator.buildColumns("w_id", "w_name", "w_street_1",
					"w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd");
			List<String> values = TPCCGenerator.buildColumns(w_id, w_name, w_street_1,
					w_street_2, w_city, w_state, w_zip, w_tax, w_ytd);
			write(TPCCConstants.TABLENAME_WAREHOUSE, key, columns, values);

			/* Make rows associated with warehouse */
			Stock(w_id);
			District(w_id);
		}
	}

	/*
	 * Function name: Stock Description: Load the stock table Argument: w_id -
	 * warehouse id
	 */
	void Stock(int w_id) {

		int s_i_id;
		int s_w_id;
		int s_quantity;
		String s_data;

		int sdatasiz;
		int orig[] = new int[TPCCConstants.NUM_ITEMS];
		int pos;

		/* Starting Loading ... */
		LOG.info("Loading Stock Wid = " + w_id);
		s_w_id = w_id;

		for (int i = 0; i < TPCCConstants.NUM_ITEMS; i++) { orig[i] = 0; }

		for (int i = 0; i < TPCCConstants.NUM_ITEMS / 10; i++) {
			do {
				pos = (new Random()).nextInt(TPCCConstants.NUM_ITEMS);
			} while (orig[pos] == 1);
			orig[pos] = 1;
		}

		for (s_i_id = 1; s_i_id <= TPCCConstants.NUM_ITEMS; s_i_id++) {
			/* Generate Stock Data */
			s_quantity = TPCCGenerator.randomInt(10, 100);
			s_data = TPCCGenerator.makeAlphaString(26, 50);
			sdatasiz = s_data.length();
			if (orig[s_i_id - 1] == 1) {
				pos = TPCCGenerator.randomInt(0, sdatasiz - 8);
				s_data = s_data.substring(0, pos) + "original" + s_data.substring(pos + 8);
			}

			/* key */
			String key = TPCCGenerator.buildString(s_w_id, "_", s_i_id);
			/* column */
			List<String> columns = TPCCGenerator.buildColumns("s_i_id", "s_w_id", "s_quantity", 
					"s_data", "s_ytd", "s_order_cnt", "s_remote_cnt");
			List<String> values = TPCCGenerator.buildColumns(String.valueOf(s_i_id), String.valueOf(s_w_id),
					String.valueOf(s_quantity), s_data, "0", "0", "0");
			for (int i = 1; i <= TPCCConstants.DISTRICTS_PER_WAREHOUSE; i++) {
				columns.add("s_dist_" + String.valueOf(i));
				values.add(TPCCGenerator.makeAlphaString(24, 24));
			}
			write(TPCCConstants.TABLENAME_STOCK, key, columns, values);
		}

		LOG.info("Stock Done.");

	}

	/*
	 * Function name: District Description: Load the district table Argument:
	 * w_id - warehouse id
	 */
	void District(int w_id) {
		/* local varibales */
		int d_id, d_w_id;
		String d_name, d_street_1, d_street_2, d_city, d_state, d_zip;
		double d_tax, d_ytd;
		int d_next_o_id;

		/* Starting Loading ... */
		LOG.info("Loading District Wid = " + w_id);

		d_w_id = w_id;
		d_ytd = TPCCConstants.INITIAL_W_YTD;
		d_next_o_id = TPCCConstants.INITIAL_NEXT_O_ID;

		for (d_id = 1; d_id <= TPCCConstants.DISTRICTS_PER_WAREHOUSE; d_id++) {
			/* Generate District Data */
			d_name = TPCCGenerator.makeAlphaString(6, 10);
			d_street_1 = TPCCGenerator.makeAlphaString(10, 20); /* Street 1 */
			d_street_2 = TPCCGenerator.makeAlphaString(10, 20); /* Street 2 */
			d_city = TPCCGenerator.makeAlphaString(10, 20); /* City */
			d_state = TPCCGenerator.makeAlphaString(2, 2); /* State */
			d_zip = TPCCGenerator.makeNumberString(9, 9); /* Zip */
			d_tax = (TPCCGenerator.randomFloat(10, 20)) / 100.0f;

			/* key */
			String key = TPCCGenerator.buildString(d_w_id, "_", d_id);
			/* column */
			List<String> columns = TPCCGenerator.buildColumns("d_id", "d_w_id", "d_name",
					"d_street_1", "d_street_2", "d_city", "d_state", "d_zip",
					"d_tax", "d_ytd", "d_next_o_id");
			List<String> values = TPCCGenerator.buildColumns(d_id, d_w_id, d_name,
					d_street_1, d_street_2, d_city, d_state, d_zip, d_tax,
					d_ytd, d_next_o_id);
			write(TPCCConstants.TABLENAME_DISTRICT, key, columns, values);
		}

		LOG.info("District Done.");

	}

	/*
	 * Function name: LoadCust Description: Call Customer() to load the Customer
	 * table Argument: none
	 */
	public void LoadCust() {

		for (int w_id = 1; w_id <= TPCCConstants.NUM_WAREHOUSE; w_id++) {
			for (int d_id = 1; d_id <= TPCCConstants.DISTRICTS_PER_WAREHOUSE; d_id++) {
				Customer(d_id, w_id);
			}
		}
		LOG.info("Customer Done.");
	}

	/*
	 * Function name: LoadCust Description: Load the customer table, the histroy
	 * table Argument: none
	 */
	void Customer(int d_id, int w_id) {
		int c_id, c_d_id, c_w_id, c_credit_lim;
		String c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip;
		String c_phone, c_since, c_credit, c_data, h_date, h_data;
		float c_discount, c_balance, h_amount;

		/* Already Set up database connection */

		for (c_id = 1; c_id <= TPCCConstants.CUSTOMERS_PER_DISTRICT; c_id++) {
			/* Generate Customer Data */
			c_d_id = d_id;
			c_w_id = w_id;
			c_first = TPCCGenerator.makeAlphaString(8, 16);
			c_middle = "OE";
			if (c_id <= 1000) {
				c_last = TPCCGenerator.Lastname(c_id - 1);
			} else {
				c_last = TPCCGenerator.Lastname(TPCCGenerator.NURand(TPCCConstants.A_C_LAST, 0, 999));
			}
			c_street_1 = TPCCGenerator.makeAlphaString(10, 20); /* Street 1 */
			c_street_2 = TPCCGenerator.makeAlphaString(10, 20); /* Street 2 */
			c_city = TPCCGenerator.makeAlphaString(10, 20); 	/* City */
			c_state = TPCCGenerator.makeAlphaString(2, 2); 		/* State */
			c_zip = TPCCGenerator.makeNumberString(9, 9); 		/* Zip */
			c_since = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
			c_phone = TPCCGenerator.makeNumberString(16, 16);
			if (TPCCGenerator.randomInt(0, 1) == 1) {
				c_credit = "GC";
			} else {
				c_credit = "BC";
			}
			c_credit_lim = 50000;
			c_discount = (TPCCGenerator.randomFloat(0, 50)) / 100.0f;
			c_balance = -10.0f;
			c_data = TPCCGenerator.makeAlphaString(300, 500);

			/* Insert into database */

			/* key */
			String key = TPCCGenerator.buildString(c_w_id, "_", c_d_id, "_", c_id);
			/* insert into the customer table */
			List<String> columns = TPCCGenerator.buildColumns("c_id", "c_d_id", "c_w_id",
					"c_first", "c_middle", "c_last", "c_street_1",
					"c_street_2", "c_city", "c_state", "c_zip", "c_phone",
					"c_since", "c_credit", "c_credit_lim", "c_discount",
					"c_balance", "c_ytd_payment", "c_payment_cnt",
					"c_delivery_cnt", "c_data");
			List<String> values = TPCCGenerator.buildColumns(c_id, c_d_id, c_w_id, c_first,
					c_middle, c_last, c_street_1, c_street_2, c_city, c_state,
					c_zip, c_phone, c_since, c_credit, c_credit_lim,
					c_discount, c_balance, 10.0f, 1, 0, c_data);
			write(TPCCConstants.TABLENAME_CUSTOMER, key, columns, values);

			/* history table data generation */
			h_amount = 10.0f;
			h_date = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
			h_data = TPCCGenerator.makeAlphaString(12, 24);

			key = UUID.randomUUID().toString();
			/* insert into the history table */
			List<String> columns_h = TPCCGenerator.buildColumns("h_c_id",
					"h_c_d_id", "h_c_w_id", "h_w_id", "h_d_id", "h_date",
					"h_amount", "h_data");
			List<String> values_h = TPCCGenerator.buildColumns(c_id, c_d_id,
					c_w_id, c_w_id, c_d_id, h_date, h_amount, h_data);
			write(TPCCConstants.TABLENAME_HISTORY, key, columns_h, values_h);

		}

	}

	/*
	 * Function name: LoadOrd Description: Call Orders() to load the order table
	 * and new_order table Argument: none
	 */
	public void LoadOrd() {
		for (int w_id = 1; w_id <= TPCCConstants.NUM_WAREHOUSE; w_id++) {
			for (int d_id = 1; d_id <= TPCCConstants.DISTRICTS_PER_WAREHOUSE; d_id++) {
				Orders(d_id, w_id);
			}
		}
		LOG.info("Order Done.");
	}

	/*
	 * Function name: Orders Description: Loads the order table as well as the
	 * order_line table Argument: d_id - district id w_id - warehouse id
	 */
	void Orders(int d_id, int w_id) {
		int o_id, o_c_id, o_d_id, o_w_id, o_carrier_id, o_ol_cnt;
		int ol, ol_i_id, ol_supply_w_id, ol_quantity;
		String o_entry_d, ol_dist_info, ol_delivery_d;
		float ol_amount;

		o_d_id = d_id;
		o_w_id = w_id;
		o_entry_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		ol_delivery_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		/* initialize permutation of customer numbers */
		int permutation[] = new int[TPCCConstants.CUSTOMERS_PER_DISTRICT];
		for (int i = 0; i < TPCCConstants.CUSTOMERS_PER_DISTRICT; i++) {
			permutation[i] = i + 1;
		}
		for (int i = 0; i < permutation.length; i++) {
			int index = (new Random()).nextInt(TPCCConstants.CUSTOMERS_PER_DISTRICT);
			int t = permutation[i];
			permutation[i] = permutation[index];
			permutation[index] = t;
		}

		for (o_id = 1; o_id <= TPCCConstants.ORDERS_PER_DISTRICT; o_id++) {
			/* Generate Order Data */
			o_c_id = permutation[o_id - 1];
			o_carrier_id = TPCCGenerator.randomInt(1, 10);
			o_ol_cnt = TPCCGenerator.randomInt(5, 15);

			String key = TPCCGenerator.buildString(o_w_id, "_", o_d_id, "_", o_id);

			List<String> columns;
			List<String> values;
			if (o_id > TPCCConstants.ORDER_INIT_DELEVERED) {
				/* orders have not been delivered to be marked as New Orders*/
				columns = TPCCGenerator.buildColumns("o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_ol_cnt", "o_all_local", "o_carrier_id");
				values = TPCCGenerator.buildColumns(o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, 1, "NULL");
				write(TPCCConstants.TABLENAME_ORDER, key, columns, values);
				/* Load new order table */
				New_Orders(o_id, o_w_id, o_d_id);
			} else {
				/* the first 2100 orders have not been delivered */
				columns = TPCCGenerator.buildColumns("o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_ol_cnt", "o_all_local", "o_carrier_id");
				values = TPCCGenerator.buildColumns(o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, 1, o_carrier_id);
				write(TPCCConstants.TABLENAME_ORDER, key, columns, values);
			}

			for (ol = 1; ol <= o_ol_cnt; ol++) {
				String key_orderline = TPCCGenerator.buildString(o_w_id, "_", o_d_id, "_", o_id, "_", ol);
				/* Generate Order Line Data */
				ol_i_id = TPCCGenerator.randomInt(1, TPCCConstants.NUM_ITEMS);
				ol_supply_w_id = o_w_id;
				ol_quantity = 5;
				ol_amount = TPCCGenerator.randomFloat(10, 10000) / 100.0f; 
				ol_dist_info = TPCCGenerator.makeAlphaString(24, 24);

				columns = TPCCGenerator.buildColumns("ol_o_id", "ol_d_id", "ol_w_id",
						"ol_number", "ol_i_id", "ol_supply_w_id",
						"ol_quantity", "ol_dist_info", "ol_amount", "ol_delivery_d");
				values = TPCCGenerator.buildColumns(o_id, o_d_id, o_w_id, ol, ol_i_id,
						ol_supply_w_id, ol_quantity, ol_dist_info, ol_amount, ol_delivery_d);
				write(TPCCConstants.TABLENAME_ORDER_LINE, key_orderline, columns, values);
			}
		}
	}

	/*
	 * Function name: New_Orders Description: Argument: none
	 */
	void New_Orders(int o_id, int no_w_id, int no_d_id) {
		String key = TPCCGenerator.buildString(no_w_id, "_", no_d_id, "_", o_id);
		List<String> columns = TPCCGenerator.buildColumns("no_o_id", "no_w_id", "no_d_id");
		List<String> values = TPCCGenerator.buildColumns(o_id, no_w_id, no_d_id);
		write(TPCCConstants.TABLENAME_NEW_ORDER, key, columns, values);
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure(ServerConfiguration.getConfiguration().getLogConfigFilePath());
		TPCCLoader loader = new TPCCLoader();
		loader.load();
	}
	

}