package com.starrocks.mysql.security;

public class CustomLdapSslSocketFactoryException extends RuntimeException {
    public CustomLdapSslSocketFactoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
