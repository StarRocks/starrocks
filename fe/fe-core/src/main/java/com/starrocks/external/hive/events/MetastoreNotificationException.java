// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.events;

/**
 * Utility exception class to be thrown for errors during event processing
 */
public class MetastoreNotificationException extends RuntimeException {

    public MetastoreNotificationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public MetastoreNotificationException(String msg) {
        super(msg);
    }

    public MetastoreNotificationException(Exception e) {
        super(e);
    }
}
