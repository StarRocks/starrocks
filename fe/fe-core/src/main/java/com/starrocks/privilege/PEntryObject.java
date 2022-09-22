// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

public interface PEntryObject {
    /**
     * if object is the same, this must be explicitly implemented
     */
    boolean isSame(PEntryObject pEntryObject);
}