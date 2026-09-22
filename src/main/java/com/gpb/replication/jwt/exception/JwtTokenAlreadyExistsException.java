package com.gpb.replication.jwt.exception;

public class JwtTokenAlreadyExistsException extends RuntimeException {

    public JwtTokenAlreadyExistsException(String service) {
        super("Active token already exists for service: " + service);
    }
}