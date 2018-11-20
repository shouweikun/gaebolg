package com.neighborhood.aka.laplace.gaebolg.exception;

/**
 * Created by john_liu on 2018/11/9.
 */
public class InvalidJsonException extends Exception{
    public InvalidJsonException() {
    }

    public InvalidJsonException(String message) {
        super(message);
    }

    public InvalidJsonException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidJsonException(Throwable cause) {
        super(cause);
    }

    public InvalidJsonException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
