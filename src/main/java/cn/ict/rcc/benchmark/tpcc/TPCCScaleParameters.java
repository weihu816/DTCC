package cn.ict.rcc.benchmark.tpcc;

/**
 * Parameters to Scale TPCC workload
 * @author Wei Hu
 *
 */
public class TPCCScaleParameters {

	public static final int NUM_WAREHOUSE = 2;				// number of warehouses - tunable 
	
	public static final int CUST_PER_DIST = 30; 			// 3000
	
	public static final int ORD_PER_DIST  = 30; 			// 3000
	public static final int ORDER_INIT_DELEVERED = 21;	// if (o_id > #e.g.2100) then it is not delivered when initilized

	// W1					W2
	// Node1	Node2		Node1	Node2
	// D1 D2	D1	D2		D1	D2	D1	D2
	public static final int NODES_PER_WAREHOUSE = 2; 								// number of node/warehouuse 
	public static final int DIST_PER_NODE = 2;										// number of districts/node
	public static final int DIST_PER_WARE = DIST_PER_NODE * NODES_PER_WAREHOUSE;	// Total number of district a single warehouse has
	
	
}
