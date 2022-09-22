// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist.metablock;

public class SRMetaBlockEOFException extends Exception {

    public SRMetaBlockEOFException(String msg) {
        super(msg);
    }
}
