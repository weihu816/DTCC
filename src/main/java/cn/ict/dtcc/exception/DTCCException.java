package cn.ict.dtcc.exception;

public class DTCCException extends RuntimeException {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DTCCException(String message) {
        super(message);
    }

    public DTCCException(String message, Throwable cause) {
        super(message, cause);
    }
}
