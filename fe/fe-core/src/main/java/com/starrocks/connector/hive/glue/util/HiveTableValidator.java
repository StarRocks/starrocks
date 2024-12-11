// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.connector.hive.glue.util;

<<<<<<< HEAD
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.Table;
import org.apache.hadoop.hive.metastore.TableType;
=======
import org.apache.hadoop.hive.metastore.TableType;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.Table;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

public enum HiveTableValidator {

    REQUIRED_PROPERTIES_VALIDATOR {
        public void validate(Table table) {
            String missingProperty = null;

            if (notApplicableTableType(table)) {
                return;
            }

<<<<<<< HEAD
            if (table.getTableType() == null) {
=======
            if (table.tableType() == null) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                missingProperty = "TableType";
            } else {
                // for iceberg table, must contain metadata location in parameters
                // TODO(zombee0), check hudi deltalake
                if (isIcebergTable(table)) {
<<<<<<< HEAD
                    if (table.getParameters().get(METADATA_LOCATION_PROP) == null) {
                        missingProperty = "MetadataLocation";
                    }
                } else if (table.getStorageDescriptor() == null) {
=======
                    if (table.parameters().get(METADATA_LOCATION_PROP) == null) {
                        missingProperty = "MetadataLocation";
                    }
                } else if (table.storageDescriptor() == null) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                    missingProperty = "StorageDescriptor";
                }
            }

            if (missingProperty != null) {
<<<<<<< HEAD
                throw new InvalidInputException(
                        String.format("%s cannot be null for table: %s", missingProperty, table.getName()));
=======
                throw InvalidInputException.builder()
                        .message(String.format("%s cannot be null for table: %s", missingProperty, table.name()))
                        .build();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        }
    };

    public static boolean isIcebergTable(Table table) {
<<<<<<< HEAD
        return table.getParameters() != null &&
                table.getParameters().get(TABLE_TYPE_PROP) != null &&
                table.getParameters().get(TABLE_TYPE_PROP).equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE);
=======
        return table.parameters() != null &&
                table.parameters().get(TABLE_TYPE_PROP) != null &&
                table.parameters().get(TABLE_TYPE_PROP).equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    public abstract void validate(Table table);

    private static boolean notApplicableTableType(Table table) {
        if (isNotManagedOrExternalTable(table) ||
                isStorageHandlerType(table)) {
            return true;
        }
        return false;
    }

    private static boolean isNotManagedOrExternalTable(Table table) {
<<<<<<< HEAD
        if (table.getTableType() != null &&
                TableType.valueOf(table.getTableType()) != TableType.MANAGED_TABLE &&
                TableType.valueOf(table.getTableType()) != TableType.EXTERNAL_TABLE) {
=======
        if (table.tableType() != null &&
                TableType.valueOf(table.tableType()) != TableType.MANAGED_TABLE &&
                TableType.valueOf(table.tableType()) != TableType.EXTERNAL_TABLE) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            return true;
        }
        return false;
    }

    private static boolean isStorageHandlerType(Table table) {
<<<<<<< HEAD
        if (table.getParameters() != null && table.getParameters().containsKey(META_TABLE_STORAGE) &&
                isNotEmpty(table.getParameters().get(META_TABLE_STORAGE))) {
=======
        if (table.parameters() != null && table.parameters().containsKey(META_TABLE_STORAGE) &&
                isNotEmpty(table.parameters().get(META_TABLE_STORAGE))) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            return true;
        }
        return false;
    }
}
