// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

public class StarRocksIcebergException extends RuntimeException {

    public StarRocksIcebergException(String msg) {
        super(msg);
    }

    public StarRocksIcebergException(String s, Throwable cause) {
        super(s, cause);
    }
}
