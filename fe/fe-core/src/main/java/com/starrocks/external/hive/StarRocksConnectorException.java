package com.starrocks.external.hive;

public class StarRocksConnectorException extends RuntimeException {
    public StarRocksConnectorException(String message) {
        super(message);
    }
}
