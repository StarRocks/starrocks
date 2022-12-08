// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.credential;

import com.starrocks.thrift.TAWSCredential;

public interface CloudCredential {

    boolean validate();

    void toThrift(TAWSCredential msg);
}
