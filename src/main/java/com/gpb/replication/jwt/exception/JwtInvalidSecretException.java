package com.gpb.replication.jwt.exception;

public class JwtInvalidSecretException extends RuntimeException {

    public JwtInvalidSecretException() {
        super("Wrong secret");
    }
}