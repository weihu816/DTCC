package cn.ict.rcc.txn;

public class TransactionException extends Exception {

	private static final long serialVersionUID = 1L;

	public TransactionException(String message) {
        super(message);
    }

    public TransactionException(String message, Throwable cause) {
        super(message, cause);
    }
}
