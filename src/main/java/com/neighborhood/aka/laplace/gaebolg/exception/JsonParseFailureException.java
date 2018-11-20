package com.neighborhood.aka.laplace.gaebolg.exception;

/**
 * Created by john_liu on 2018/11/9.
 */
public class JsonParseFailureException extends Exception {
    public JsonParseFailureException() {
    }

    public JsonParseFailureException(String message) {
        super(message);
    }

    public JsonParseFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public JsonParseFailureException(Throwable cause) {
        super(cause);
    }

    public JsonParseFailureException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
