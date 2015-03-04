package cn.ict.rcc.exception;

public class RococoException extends RuntimeException {

    public RococoException(String message) {
        super(message);
    }

    public RococoException(String message, Throwable cause) {
        super(message, cause);
    }
}
