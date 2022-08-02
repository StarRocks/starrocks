// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

public interface DeleteTableAction {
    // @return: true if the associated table can be removed from the CatalogRecycleBin, false otherwise.
    boolean execute();
}
