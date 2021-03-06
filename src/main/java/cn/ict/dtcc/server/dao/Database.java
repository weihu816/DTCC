package cn.ict.dtcc.server.dao;

import java.util.HashMap;
import java.util.List;

import cn.ict.dtcc.benchmark.tpcc.TPCCGenerator;

/**
 * The interface of a Database that includes all the operations need for tpcc
 * benchmark
 * @author Wei Hu
 */

public interface Database {

	public static final List<String> DELETE_NAME = TPCCGenerator.buildColumns("__DELETE__");
	public static final List<String> DELETE_VALUE = TPCCGenerator.buildColumns("__DELETE__");
	public static final String DELETE_FIELD = "__DELETE__";
	public static final String EXIST_STRING = "__EXIST__";
	public final static int WRITE = 1;
	public final static int DELETE = 2;

	public void init();
	public void shutdown();
	
	public void createTables(List<String> tables);
	
	public TransactionRecord getTransactionRecord(String transactionId);
	
	public void putTransactionRecord(TransactionRecord record);
	
	public Record get(String table, String key);
	
	public void put(Record record);
	public void weakPut(Record record);
	
	public boolean createSecondaryIndex(String table, List<String> fields);
	public boolean deleteSecondaryIndex(String table, String key);
	
	public boolean write(String table, String key, List<String> names, List<String> values);
	
	public boolean delete(String table, String key);

//	public HashMap<String, String> read(String table, String key,
//			List<String> names);
//
//	/**
//	 * Retrieve information from database
//	 * 
//	 * @param table
//	 *            the name of the table
//	 * @param key
//	 *            the key to identify the row
//	 * @param columns
//	 *            column the fields to retrieve
//	 * @return a hash map containing the information retrieved
//	 */
//	public HashMap<String, String> read(String table, String key,
//			String columns[]);
//
//	/**
//	 * Retrieve from database if constraintColumn is null then no constraint if
//	 * orderColumn is null then no sorting required
//	 * 
//	 * @param table
//	 *            the name of the table
//	 * @param key_prefix
//	 *            the prefix of the key to identify rows
//	 * @param ConstraintValue
//	 *            exclusive upperBound
//	 * @return : a hash map containing the information retrieved
//	 */
//	public List<HashMap<String, String>> read(String table, String key_prefix,
//			String columns[], String constraintColumn, String constraintValue,
//			String orderColumn, boolean isAssending);
//
//	/**
//	 * Select with constraints and get projection on argument "column"
//	 * 
//	 * @param table
//	 *            the name of the table
//	 * @param key_prefix
//	 *            the prefix of the key to identify rows
//	 * @param constraintColumn
//	 *            the column to impose constraints
//	 * @param lowerBound
//	 *            inclusive
//	 * @param upperBound
//	 *            exclusive
//	 * @return a List of String containing the information retrieved
//	 */
//	public List<String> read(String table, String key_prefix,
//			String projectionColumn, String constraintColumn, int lowerBound,
//			int upperBound);
//
//	/**
//	 * Select with constraints and get projection on argument "column"
//	 * 
//	 * @param action
//	 *            0insert; 1update; 2delete
//	 * 
//	 */
//	public boolean write(String table, String key, List<String> names,
//			List<String> values);

}
