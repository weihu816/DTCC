package cn.ict.dtcc.exception;

public class OCCException extends RuntimeException {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public OCCException(String message) {
        super(message);
    }

    public OCCException(String message, Throwable cause) {
        super(message, cause);
    }
}
