package cn.ict.rcc.benchmark.tpcc;

import java.util.Random;

/**
 * Holds TPC-C constants
 * @author Wei Hu
 */

public final class TPCCConstants {

    private TPCCConstants() { assert false; }
    
    public static final int NUM_WAREHOUSE = 1;				// number of warehouses - tunable 
	public static final int ORDER_INIT_DELEVERED = 21;		// if (o_id > #e.g.2100) then it is not delivered when initilized
	public static final int NODES_PER_WAREHOUSE = 8; 								// number of node/warehouuse 
	
	
    public static final String TABLENAME_WAREHOUSE 	= "WAREHOUSE";
    public static final String TABLENAME_DISTRICT 	= "DISTRICT";
    public static final String TABLENAME_ITEM 		= "ITEM";
    public static final String TABLENAME_CUSTOMER 	= "CUSTOMER";
    public static final String TABLENAME_HISTORY 	= "HISTORY";
    public static final String TABLENAME_STOCK 		= "STOCK";
    public static final String TABLENAME_ORDER 		= "ORDERS";
    public static final String TABLENAME_NEW_ORDER 	= "NEW_ORDER";
    public static final String TABLENAME_ORDER_LINE = "ORDER_LINE";
	
    public static final String TABLENAMES[] = {
        TABLENAME_WAREHOUSE,
        TABLENAME_DISTRICT,
        TABLENAME_ITEM,
        TABLENAME_CUSTOMER,
        TABLENAME_HISTORY,
        TABLENAME_STOCK,
        TABLENAME_ORDER,
        TABLENAME_NEW_ORDER,
        TABLENAME_ORDER_LINE,
    };

    public static final int FREQUENCY_STOCK_LEVEL 	= 4;
    public static final int FREQUENCY_DELIVERY 		= 4;
    public static final int FREQUENCY_ORDER_STATUS 	= 4;
    public static final int FREQUENCY_PAYMENT 		= 43;
    public static final int FREQUENCY_NEW_ORDER 	= 45;
    

    // 2 digits after the decimal point for money types
    public static final int MONEY_DECIMALS = 2;

	// skew constants
	public static final int HOT_DATA_WORKLOAD_SKEW = 100;
	public static final int HOT_DATA_SIZE = 0;
	public static final int WARM_DATA_SIZE = 0;
	public static final int WARM_DATA_WORKLOAD_SKEW = 0;

	// Percentage of neworder txns that will abort
	public static final int NEWORDER_ABORT = 1; // 1%

    // ITEM constants
	public static final int 	STARTING_ITEM 	= 0;
    public static final int 	NUM_ITEMS 	  	= 1000;		//100000
    public static final int 	MIN_IM 			= 1;
    public static final int 	MAX_IM 			= 10000;
    public static final double 	MIN_PRICE 		= 1.00;
    public static final double 	MAX_PRICE 		= 100.00;
    public static final int 	MIN_I_NAME 		= 14;
    public static final int 	MAX_I_NAME 		= 24;
    public static final int 	MIN_I_DATA 		= 26;
    public static final int 	MAX_I_DATA 		= 50;

    // WAREHOUSE constants
    public static final int 	STARTING_WAREHOUSE 	= 1;
    public static final double 	MIN_TAX 			= 0;
    public static final double 	MAX_TAX 			= 0.2000;
    public static final int 	TAX_DECIMALS 		= 4;
    public static final double 	INITIAL_W_YTD 		= 300000.00; // 300000.00? TODO: verify
    public static final int 	MIN_NAME 			= 6;
    public static final int 	MAX_NAME 			= 10;
    public static final int 	MIN_STREET 			= 10;
    public static final int 	MAX_STREET 			= 20;
    public static final int 	MIN_CITY 			= 10;
    public static final int 	MAX_CITY 			= 20;
    public static final int 	STATE 				= 2;
    public static final int 	ZIP_LENGTH 			= 9;
    public static final String 	ZIP_SUFFIX 			= "11111";

    // STOCK constants
    public static final int MIN_QUANTITY = 10;
    public static final int MAX_QUANTITY = 100;
    public static final int DIST = 24;
    public static final int STOCK_PER_WAREHOUSE = 100000;

    // DISTRICT constants
    public static final double INITIAL_D_YTD 		= 30000.00;  // different from Warehouse
    public static final int INITIAL_NEXT_O_ID 		= 31;		 // 3001
    public static final int DISTRICTS_PER_NODE = 10;											// number of districts/node
	public static final int DISTRICTS_PER_WAREHOUSE = DISTRICTS_PER_NODE * NODES_PER_WAREHOUSE;	// 10 * 8

    // CUSTOMER constants
    public static final int CUSTOMERS_PER_DISTRICT = 30;						// 3000
    public static final double INITIAL_CREDIT_LIM = 50000.00;
    public static final double MIN_DISCOUNT = 0.0000;
    public static final double MAX_DISCOUNT = 0.5000;
    public static final int DISCOUNT_DECIMALS = 4;
    public static final double INITIAL_BALANCE = -10.00;
    public static final double INITIAL_YTD_PAYMENT = 10.00;
    public static final int INITIAL_PAYMENT_CNT = 1;
    public static final int INITIAL_DELIVERY_CNT = 0;
    public static final int MIN_FIRST = 6;
    public static final int MAX_FIRST = 10;
    public static final String MIDDLE = "OE";
    public static final int PHONE = 16;
    public static final int MIN_C_DATA = 300;
    public static final int MAX_C_DATA = 500;
    public static final String GOOD_CREDIT = "GC";
    public static final String BAD_CREDIT = "BC";
    public static final byte[] BAD_CREDIT_BYTES = BAD_CREDIT.getBytes();

    // ORDERS constants
    public static final int ORDERS_PER_DISTRICT = 30;					//	3000
    public static final int MIN_CARRIER_ID = 1;
    public static final int MAX_CARRIER_ID = 10;
    // HACK: This is not strictly correct, but it works
    public static final long NULL_CARRIER_ID = 0L;
    // o_id < than this value, carrier != null, >= -> carrier == null
    public static final int NULL_CARRIER_LOWER_BOUND = 2101;
    public static final int MIN_OL_CNT = 5;
    public static final int MAX_OL_CNT = 15;
    public static final int INITIAL_ALL_LOCAL = 1;
    // Used to generate new order transactions
    public static final int MAX_OL_QUANTITY = 10;

    // ORDER LINE constants
    public static final int INITIAL_QUANTITY = 5;
    public static final double MIN_AMOUNT = 0.01;

    // HISTORY constants
    public static final int MIN_DATA = 12;
    public static final int MAX_DATA = 24;
    public static final double INITIAL_AMOUNT = 10.00f;

    // NEW ORDER constants
    public static final int INITIAL_NEW_ORDERS_PER_DISTRICT = 900;

    // TPC-C 2.4.3.4 (page 31) says this must be displayed when new order rolls back.
    public static final String INVALID_ITEM_MESSAGE = "Item number is not valid";

    // Used to generate stock level transactions
    public static final int MIN_STOCK_LEVEL_THRESHOLD = 10;
    public static final int MAX_STOCK_LEVEL_THRESHOLD = 20;

    // Used to generate payment transactions
    public static final double MIN_PAYMENT = 1.0;
    public static final double MAX_PAYMENT = 5000.0;

    // Indicates "brand" items and stock in i_data and s_data.
    public static final String ORIGINAL_STRING = "ORIGINAL";
    public static final byte[] ORIGINAL_BYTES = ORIGINAL_STRING.getBytes();
    
    /* NURand */
	public static final int A_C_LAST 	= 255;
	public static final int A_C_ID 		= 1023;
	public static final int A_OL_I_ID 	= 8191;
	public static final int C_C_LAST 	= randomInt(0, A_C_LAST);
	public static final int C_C_ID 		= randomInt(0, A_C_ID);
	public static final int C_OL_I_ID 	= randomInt(0, A_OL_I_ID);
	
	public static int randomInt(int min, int max) {
		return new Random().nextInt(max + 1) % (max - min + 1) + min;
	}

	public static float randomFloat(float min, float max) {
		return new Random().nextFloat() * (max - min) + min;
	}
}
