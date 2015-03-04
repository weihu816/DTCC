package cn.ict.rcc.txn;

public class TransactionFactory {
    
    public RococoTransaction create() {
    	return new RococoTransaction();
    }

}
