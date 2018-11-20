package com.neighborhood.aka.laplace.gaebolg.exception;

/**
 * Created by john_liu on 2018/10/29.
 */
public class InvalidSchemaJsonException extends Exception{


    public InvalidSchemaJsonException() {
    }

    public InvalidSchemaJsonException(String message) {
        super(message);
    }

    public InvalidSchemaJsonException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidSchemaJsonException(Throwable cause) {
        super(cause);
    }

    public InvalidSchemaJsonException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
