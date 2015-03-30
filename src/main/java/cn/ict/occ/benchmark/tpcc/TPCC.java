package cn.ict.occ.benchmark.tpcc;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import cn.ict.dtcc.benchmark.tpcc.TPCCConstants;
import cn.ict.dtcc.benchmark.tpcc.TPCCGenerator;
import cn.ict.dtcc.config.ServerConfiguration;
import cn.ict.dtcc.exception.TransactionException;
import cn.ict.occ.txn.OCCTransaction;
import cn.ict.occ.txn.TransactionFactory;

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
		OCCTransaction transaction = transactionFactory.create();
		transaction.begin();
		
		List<String> columns, values;
		int c_id = TPCCGenerator.NURand(TPCCConstants.A_C_ID, 1, TPCCConstants.CUSTOMERS_PER_DISTRICT);
		
		// Ri&R District : increase d_next_o_id
		String key_district = (TPCCGenerator.buildString(w_id, "_", d_id));
		columns = TPCCGenerator.buildColumns("d_next_o_id", "d_tax");
		values = transaction.read(TPCCConstants.TABLENAME_DISTRICT, key_district, columns);

		LOG.debug("d_next_o_id = " + values.get(0));
		LOG.debug("d_tax = " + values.get(1));

//		// PIECE 2 R warehouse
//		// read w_tax, immediate ! READONLY
//		String key_warehouse = String.valueOf(w_id);
//		columns = TPCCGenerator.buildColumns("w_tax");
//		int pieceNum_warehouse = transaction.createPiece(TPCCConstants.TABLENAME_WAREHOUSE, key_warehouse, true);
//		transaction.readSelect(columns);
//		transaction.completePiece();
//
//		float w_tax = Float.valueOf(transaction.get(pieceNum_warehouse, "w_tax"));
//		LOG.debug("Piece 2: R warehouse");
//		LOG.debug("w_tax: " + w_tax);
//		
//		// PIECE 3 R customer
//		String key_customer = TPCCGenerator.buildString(w_id, "_", d_id, "_", c_id);
//		columns = TPCCGenerator.buildColumns ("c_last", "c_discount", "c_credit");
//		int pieceNum_customer = transaction.createPiece(TPCCConstants.TABLENAME_CUSTOMER, key_customer, true);
//		transaction.readSelect(columns);
//		transaction.completePiece();
//		float c_discount = Float.valueOf(transaction.get(pieceNum_customer, "c_discount"));
//		String c_last = transaction.get(pieceNum_customer, "c_last");
//		String c_credit = transaction.get(pieceNum_customer, "c_credit");
//		LOG.debug("Piece 3: R customer");
//		LOG.debug("c_discount: " + c_discount);
//		LOG.debug("c_last: " + c_last);
//		LOG.debug("c_credit: " + c_credit);
//		//------------------------------------------------------------------
//		int o_all_local = 1, o_ol_cnt = TPCCGenerator.randomInt(5, 15);
//		int 	supware			[] 	= new int	[o_ol_cnt];
//		int 	ol_i_ids		[] 	= new int	[o_ol_cnt];
//		String i_names			[] 	= new String[o_ol_cnt];
//		float i_prices			[] = new float[o_ol_cnt];
//		float ol_amounts		[] = new float[o_ol_cnt];
//		int ol_quantities		[] = new int[o_ol_cnt];
//		int s_quantities		[] = new int[o_ol_cnt];
//		char bg[] = new char[o_ol_cnt];
//		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
//			int ol_supply_w_id; 
//			/* 90% of supply are from home stock */
//			if (TPCCGenerator.randomInt(0, 99) < 10 && TPCCConstants.NUM_WAREHOUSE > 1) {
//				int supply_w_id = TPCCGenerator.randomInt(1, TPCCConstants.NUM_WAREHOUSE); 
//				while (supply_w_id == w_id) { supply_w_id = TPCCGenerator.randomInt(1, TPCCConstants.NUM_WAREHOUSE); }
//				ol_supply_w_id = supply_w_id;
//			} else { ol_supply_w_id = w_id; }
//			if (ol_supply_w_id != w_id) { o_all_local = 0; }
//			supware[ol_number - 1] 	= ol_supply_w_id;
//			ol_i_ids[ol_number - 1] = TPCCGenerator.NURand(TPCCConstants.A_OL_I_ID,1, TPCCConstants.NUM_ITEMS);
//		}
//		String o_entry_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
//		//------------------------------------------------------------------
//		
//		
//		// PIECE 4 W order
//		String key_order = TPCCGenerator.buildString(w_id, "_", d_id, "_", o_id);
//		transaction.createPiece(TPCCConstants.TABLENAME_ORDER, key_order, false);
//		columns = TPCCGenerator.buildColumns("o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_id", "o_carrier_id", "o_ol_cnt", "o_all_local");
//		values = TPCCGenerator.buildColumns((o_id), (d_id), (w_id), (c_id), (o_entry_d), "NULL", (o_ol_cnt), (o_all_local));
//		transaction.write(columns, values);
//		transaction.completePiece();
//		LOG.debug("Piece 4: W order");
//		
//		// PIECE 5 W new_order
//		String key_newOrder = key_order;
//		transaction.createPiece(TPCCConstants.TABLENAME_NEW_ORDER, key_newOrder, false);
//		columns = TPCCGenerator.buildColumns("no_o_id", "no_d_id", "no_w_id");
//		values = TPCCGenerator.buildColumns(o_id, d_id, w_id);
//		transaction.write(columns, values);
//		transaction.completePiece();
//		// TODO :index
//		LOG.debug("Piece 5: W new_order");
//
//		/* for each order in the order line*/
//		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
//			//------------------------------------------------------------------
//			int ol_supply_w_id 	= supware[ol_number - 1]; 
//			int ol_i_id 		= ol_i_ids[ol_number - 1]; 
//			int ol_quantity 	= TPCCGenerator.randomInt(1, 10);
//			//------------------------------------------------------------------
//			// Piece 6 Ri item
//			String key_item = String.valueOf(ol_i_id);
//			columns = TPCCGenerator.buildColumns("i_price", "i_name", "i_data");
//			int pieceNum_item = transaction.createPiece(TPCCConstants.TABLENAME_ITEM, key_item, true);
//			transaction.readSelect(columns);
//			transaction.completePiece();
//			String i_name = transaction.get(pieceNum_item, "i_name");
//			float i_price = Float.valueOf(transaction.get(pieceNum_item, "i_price"));
//			String i_data = transaction.get(pieceNum_item, "i_data");
//			LOG.debug("Piece 6: Ri item | orderline#: " + ol_number);
//			LOG.debug("i_name: " + i_name);
//			LOG.debug("i_price: " + i_price);
//			LOG.debug("i_data: " + i_data);
//			
//			
//			// Piece 7 Ri stock
//			// 可能会有很小概率的冲突
//			/* update stock quantity */
//			String key_stock = TPCCGenerator.buildString(ol_supply_w_id, "_", ol_i_id);
//			int pieceNum_stock = transaction.createPiece(TPCCConstants.TABLENAME_STOCK, key_stock, true);
//			/* retrieve stock information */
//			columns = TPCCGenerator.buildColumns("s_quantity", "s_data");
//			columns.add("s_dist_" +  d_id);
//			transaction.readSelect(columns);
//			transaction.completePiece();
//			String ol_dist_info = transaction.get(pieceNum_stock, "s_dist_" +  d_id);
//			String s_data = transaction.get(pieceNum_stock, "s_data");
//			int s_quantity = Integer.valueOf(transaction.get(pieceNum_stock, "s_quantity"));
//			LOG.debug("Piece 7: Ri stock | orderline#: " + ol_number);
//			LOG.debug("ol_dist_info: " + ol_dist_info);
//			LOG.debug("s_data: " + s_data);
//			LOG.debug("s_quantity: " + s_quantity);
//			
//			if ( i_data != null && s_data != null && (i_data.indexOf("original") != -1) && (s_data.indexOf("original") != -1) ) {
//				bg[ol_number-1] = 'B'; 
//			} else {
//				bg[ol_number-1] = 'G';
//			}
//			
//			// Piece 8  W stock
//			pieceNum_stock = transaction.createPiece(TPCCConstants.TABLENAME_STOCK, key_stock, false);
//			if (s_quantity > ol_quantity) {
//				s_quantity = s_quantity - ol_quantity;
//			} else {
//				s_quantity = s_quantity - ol_quantity + 91;
//			}
//			transaction.write("s_quantity", String.valueOf(s_quantity));
//			transaction.completePiece();
//			LOG.debug("Piece 8: W stock | orderline#: " + ol_number);
//
//			// Piece 9  W order_line			
//			float ol_amount = ol_quantity * i_price *(1+w_tax+d_tax) *(1-c_discount); 
//			String key_orderline = TPCCGenerator.buildString(w_id, "_", d_id , "_" , o_id , "_", ol_number);
//			transaction.createPiece(TPCCConstants.TABLENAME_ORDER_LINE, key_orderline, false);
//			columns = TPCCGenerator.buildColumns("ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_id",
//					"ol_quantity", "ol_amount", "ol_dist_info");
//			values = TPCCGenerator.buildColumns(o_id, d_id, w_id, ol_number, ol_i_id,
//					ol_supply_w_id, "NULL", ol_quantity, ol_amount, ol_dist_info);
//			transaction.write(columns, values);
//			transaction.completePiece();
//			LOG.debug("Piece 9: W order_line | orderline#: " + ol_number);
//
//			i_names			[ol_number - 1] = i_name;
//			i_prices		[ol_number - 1] = i_price;
//			ol_amounts		[ol_number - 1] = ol_amount;
//			ol_quantities	[ol_number - 1] = ol_quantity;
//			s_quantities	[ol_number - 1] = s_quantity;
//		}
//
//		transaction.commit();
//		
//		boolean valid = true;
//		LOG.debug("==============================New Order==================================");
//		LOG.debug("Warehouse: " + w_id + "\tDistrict: " + d_id);
//		if (valid) {
//			LOG.debug("Customer: " + c_id + "\tName: " + c_last + "\tCredit: " + c_credit + "\tDiscount: " + c_discount);
//			LOG.debug("Order Number: " + o_id + " OrderId: " + o_id + " Number_Lines: " + o_ol_cnt + " W_tax: " + w_tax + " D_tax: " + d_tax + "\n");
//			LOG.debug("Supp_W Item_Id           Item Name     ol_q s_q  bg Price Amount");
//			for (int i = 0; i < o_ol_cnt; i++) {
//				LOG.debug( String.format("  %4d %6d %24s %2d %4d %3c %6.2f %6.2f",
//						supware[i], ol_i_ids[i], i_names[i], ol_quantities[i], s_quantities[i], bg[i], i_prices[i], ol_amounts[i]));
//			}
//		} else {
//			LOG.debug("Customer: " + c_id + "\tName: " + c_last + "\tCredit: " + c_credit + "\tOrderId: " + o_id);
//			LOG.debug("Exection Status: Item number is not valid");
//		}
//		LOG.debug("=========================================================================");
		
	}
	

	
	public static void main(String[] args) {

		PropertyConfigurator.configure(ServerConfiguration.getConfiguration().getLogConfigFilePath());
		try {
			TPCC.Neworder(1, 1);
		} catch (TransactionException e) {
			e.printStackTrace();
		}
	}
}

