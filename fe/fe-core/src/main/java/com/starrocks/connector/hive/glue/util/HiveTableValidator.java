// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.glue.util;

import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.Table;
import org.apache.hadoop.hive.metastore.TableType;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

public enum HiveTableValidator {

    REQUIRED_PROPERTIES_VALIDATOR {
        public void validate(Table table) {
            String missingProperty = null;

            if (notApplicableTableType(table)) {
                return;
            }

            if (table.getTableType() == null) {
                missingProperty = "TableType";
            } else if (table.getStorageDescriptor() == null) {
                missingProperty = "StorageDescriptor";
            }

            if (missingProperty != null) {
                throw new InvalidInputException(
                        String.format("%s cannot be null for table: %s", missingProperty, table.getName()));
            }
        }
    };

    public abstract void validate(Table table);

    private static boolean notApplicableTableType(Table table) {
        if (isNotManagedOrExternalTable(table) ||
                isStorageHandlerType(table)) {
            return true;
        }
        return false;
    }

    private static boolean isNotManagedOrExternalTable(Table table) {
        if (table.getTableType() != null &&
                TableType.valueOf(table.getTableType()) != TableType.MANAGED_TABLE &&
                TableType.valueOf(table.getTableType()) != TableType.EXTERNAL_TABLE) {
            return true;
        }
        return false;
    }

    private static boolean isStorageHandlerType(Table table) {
        if (table.getParameters() != null && table.getParameters().containsKey(META_TABLE_STORAGE) &&
                isNotEmpty(table.getParameters().get(META_TABLE_STORAGE))) {
            return true;
        }
        return false;
    }
}
