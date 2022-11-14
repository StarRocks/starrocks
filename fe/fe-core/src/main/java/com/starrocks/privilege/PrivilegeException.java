// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

public class PrivilegeException extends Exception {
    public PrivilegeException(String msg) {
        super(msg);
    }
}
