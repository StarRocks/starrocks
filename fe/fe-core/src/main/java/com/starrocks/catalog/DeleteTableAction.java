// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

public interface DeleteTableAction {
    // @return: true on success, false otherwise.
    boolean execute();
}
