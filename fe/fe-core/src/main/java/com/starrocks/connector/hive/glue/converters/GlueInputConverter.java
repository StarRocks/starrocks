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


package com.starrocks.connector.hive.glue.converters;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class provides methods to convert Hive/Catalog objects to Input objects used
 * for Glue API parameters
 */
public final class GlueInputConverter {

    public static DatabaseInput convertToDatabaseInput(Database hiveDatabase) {
        return convertToDatabaseInput(HiveToCatalogConverter.convertDatabase(hiveDatabase));
    }

    public static DatabaseInput convertToDatabaseInput(software.amazon.awssdk.services.glue.model.Database database) {
        DatabaseInput.Builder input = DatabaseInput.builder();

        input.name(database.name());
        input.description(database.description());
        input.locationUri(database.locationUri());
        input.parameters(database.parameters());

        return input.build();
    }

    public static TableInput convertToTableInput(Table hiveTable) {
        return convertToTableInput(HiveToCatalogConverter.convertTable(hiveTable));
    }

    public static TableInput convertToTableInput(software.amazon.awssdk.services.glue.model.Table table) {
        TableInput.Builder tableInput = TableInput.builder();

        tableInput.retention(table.retention());
        tableInput.partitionKeys(table.partitionKeys());
        tableInput.tableType(table.tableType());
        tableInput.name(table.name());
        tableInput.owner(table.owner());
        tableInput.lastAccessTime(table.lastAccessTime());
        tableInput.storageDescriptor(table.storageDescriptor());
        tableInput.parameters(table.parameters());
        tableInput.viewExpandedText(table.viewExpandedText());
        tableInput.viewOriginalText(table.viewOriginalText());

        return tableInput.build();
    }

    public static PartitionInput convertToPartitionInput(Partition src) {
        return convertToPartitionInput(HiveToCatalogConverter.convertPartition(src));
    }

    public static PartitionInput convertToPartitionInput(software.amazon.awssdk.services.glue.model.Partition src) {
        PartitionInput.Builder partitionInput = PartitionInput.builder();

        partitionInput.lastAccessTime(src.lastAccessTime());
        partitionInput.parameters(src.parameters());
        partitionInput.storageDescriptor(src.storageDescriptor());
        partitionInput.values(src.values());

        return partitionInput.build();
    }

    public static List<PartitionInput> convertToPartitionInputs(
            Collection<software.amazon.awssdk.services.glue.model.Partition> parts) {
        List<PartitionInput> inputList = new ArrayList<>();

        for (software.amazon.awssdk.services.glue.model.Partition part : parts) {
            inputList.add(convertToPartitionInput(part));
        }
        return inputList;
    }

    public static UserDefinedFunctionInput convertToUserDefinedFunctionInput(Function hiveFunction) {
        UserDefinedFunctionInput.Builder functionInput = UserDefinedFunctionInput.builder();

        functionInput.className(hiveFunction.getClassName());
        functionInput.functionName(hiveFunction.getFunctionName());
        functionInput.ownerName(hiveFunction.getOwnerName());
        if (hiveFunction.getOwnerType() != null) {
            functionInput.ownerType(hiveFunction.getOwnerType().name());
        }
        functionInput.resourceUris(HiveToCatalogConverter.covertResourceUriList(hiveFunction.getResourceUris()));
        return functionInput.build();
    }

}
