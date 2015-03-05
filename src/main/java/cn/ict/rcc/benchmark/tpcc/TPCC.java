package cn.ict.rcc.benchmark.tpcc;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import cn.ict.rcc.server.coordinator.txn.CoordinatorClient;
import cn.ict.rcc.server.coordinator.txn.CoordinatorClientConfiguration;
import cn.ict.rcc.server.coordinator.txn.RococoTransaction;
import cn.ict.rcc.server.coordinator.txn.TransactionException;
import cn.ict.rcc.server.coordinator.txn.TransactionFactory;

/**
 * TPCC Transaction Stored Procedures
 * @author Wei
 *
 */
public class TPCC {

	private static final Log LOG = LogFactory.getLog(TPCC.class);
	
	static Random random = new Random();
	
	public static int COUNT_WARE = 1;
	
	static TransactionFactory transactionFactory = new TransactionFactory();
	
	
	public TPCC() {
		
	}

	public static void Neworder(int w_id, int d_id) throws TransactionException {
		
		// begin
		RococoTransaction transaction = transactionFactory.create();
		transaction.begin();
		
		List<String> columns, values;
		int c_id = TPCCGenerator.NURand(TPCCConstants.A_C_ID, 1, TPCCScaleParameters.CUST_PER_DIST);
		
		// PIECE 1
		// increase d_next_o_id, immediate
		String key_district = (TPCCGenerator.buildString(w_id, "_", d_id));
		int pieceNum_district = transaction.createPiece(TPCCConstants.TABLENAME_DISTRICT, key_district, true);
		columns = TPCCGenerator.buildColumns("d_next_o_id", "d_tax");
		transaction.readSelect(columns);
		transaction.addvalue("d_next_o_id", 1);
		transaction.completePiece();

		int o_id = Integer.parseInt(transaction.get(pieceNum_district, "d_next_o_id"));
		float d_tax = Float.valueOf(transaction.get(pieceNum_district, "d_tax"));
		LOG.info(o_id);
		LOG.info(d_tax);
		
		// PIECE 2
		// read w_tax, immediate ! READONLY
		String key_warehouse = String.valueOf(w_id);
		columns = TPCCGenerator.buildColumns("w_tax");
		int pieceNum_warehouse = transaction.createPiece(TPCCConstants.TABLENAME_WAREHOUSE, key_warehouse, true);
		transaction.readSelect(columns);
		transaction.completePiece();

		float w_tax = Float.valueOf(transaction.get(pieceNum_warehouse, "w_tax"));
		LOG.info(w_tax);
		

		// PIECE 3
		// read
		String key_customer = TPCCGenerator.buildString(w_id, "_", d_id, "_", c_id);
		columns = TPCCGenerator.buildColumns ("c_discount");
		int pieceNum_customer = transaction.createPiece(TPCCConstants.TABLENAME_CUSTOMER, key_customer, true);
		transaction.readSelect(columns);
		transaction.completePiece();
		

		//------------------------------------------------------------------
		int o_all_local = 1, o_ol_cnt = TPCCGenerator.randomInt(5, 15);
		int 	supware			[] 	= new int	[o_ol_cnt];
		int 	ol_i_ids		[] 	= new int	[o_ol_cnt];
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
			int ol_supply_w_id; 
			/* 90% of supply are from home stock */
			if (TPCCGenerator.randomInt(0, 99) < 10 && COUNT_WARE > 1) {
				int supply_w_id = TPCCGenerator.randomInt(1, COUNT_WARE); 
				while (supply_w_id == w_id) { supply_w_id = TPCCGenerator.randomInt(1, COUNT_WARE); }
				ol_supply_w_id = supply_w_id;
			} else { ol_supply_w_id = w_id; }
			if (ol_supply_w_id != w_id) { o_all_local = 0; }
			supware[ol_number - 1] 	= ol_supply_w_id;
			ol_i_ids[ol_number - 1] = TPCCGenerator.NURand(TPCCConstants.A_OL_I_ID,1,100000);
		}
		String o_entry_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		//------------------------------------------------------------------
		
		
		// PIECE 2 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		/* insert into order table*/
		String key_order = TPCCGenerator.buildString(w_id, "_", d_id, "_", o_id);
		transaction.createPiece(TPCCConstants.TABLENAME_ORDER, key_order, false);
		columns = TPCCGenerator.buildColumns("o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_id", "o_carrier_id", "o_ol_cnt", "o_all_local");
		values = TPCCGenerator.buildColumns((o_id), (d_id), (w_id), (c_id), (o_entry_d), "NULL", (o_ol_cnt), (o_all_local));
		transaction.write(columns, values);
		transaction.completePiece();
		// PIECE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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
	
	public static void main(String[] args) {

		PropertyConfigurator.configure(CoordinatorClientConfiguration
				.getConfiguration().getLogConfigFilePath());

		CoordinatorClient client = new CoordinatorClient();
		client.NewOrder(1, 1);

	}
}
