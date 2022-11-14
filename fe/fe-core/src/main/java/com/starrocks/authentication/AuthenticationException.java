// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.authentication;

public class AuthenticationException extends Exception {
    public AuthenticationException(String msg) {
        super(msg);
    }
}
