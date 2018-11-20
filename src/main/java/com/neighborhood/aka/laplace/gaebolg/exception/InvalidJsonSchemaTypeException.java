package com.neighborhood.aka.laplace.gaebolg.exception;

/**
 * Created by john_liu on 2018/10/30.
 */
public class InvalidJsonSchemaTypeException extends Exception {
    public InvalidJsonSchemaTypeException() {
    }

    public InvalidJsonSchemaTypeException(String message) {
        super(message);
    }

    public InvalidJsonSchemaTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidJsonSchemaTypeException(Throwable cause) {
        super(cause);
    }

    public InvalidJsonSchemaTypeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
