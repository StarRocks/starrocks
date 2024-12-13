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

<<<<<<< HEAD
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
<<<<<<< HEAD
=======
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

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

<<<<<<< HEAD
    public static DatabaseInput convertToDatabaseInput(com.amazonaws.services.glue.model.Database database) {
        DatabaseInput input = new DatabaseInput();

        input.setName(database.getName());
        input.setDescription(database.getDescription());
        input.setLocationUri(database.getLocationUri());
        input.setParameters(database.getParameters());

        return input;
=======
    public static DatabaseInput convertToDatabaseInput(software.amazon.awssdk.services.glue.model.Database database) {
        DatabaseInput.Builder input = DatabaseInput.builder();

        input.name(database.name());
        input.description(database.description());
        input.locationUri(database.locationUri());
        input.parameters(database.parameters());

        return input.build();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public static TableInput convertToTableInput(Table hiveTable) {
        return convertToTableInput(HiveToCatalogConverter.convertTable(hiveTable));
    }

<<<<<<< HEAD
    public static TableInput convertToTableInput(com.amazonaws.services.glue.model.Table table) {
        TableInput tableInput = new TableInput();

        tableInput.setRetention(table.getRetention());
        tableInput.setPartitionKeys(table.getPartitionKeys());
        tableInput.setTableType(table.getTableType());
        tableInput.setName(table.getName());
        tableInput.setOwner(table.getOwner());
        tableInput.setLastAccessTime(table.getLastAccessTime());
        tableInput.setStorageDescriptor(table.getStorageDescriptor());
        tableInput.setParameters(table.getParameters());
        tableInput.setViewExpandedText(table.getViewExpandedText());
        tableInput.setViewOriginalText(table.getViewOriginalText());

        return tableInput;
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public static PartitionInput convertToPartitionInput(Partition src) {
        return convertToPartitionInput(HiveToCatalogConverter.convertPartition(src));
    }

<<<<<<< HEAD
    public static PartitionInput convertToPartitionInput(com.amazonaws.services.glue.model.Partition src) {
        PartitionInput partitionInput = new PartitionInput();

        partitionInput.setLastAccessTime(src.getLastAccessTime());
        partitionInput.setParameters(src.getParameters());
        partitionInput.setStorageDescriptor(src.getStorageDescriptor());
        partitionInput.setValues(src.getValues());

        return partitionInput;
    }

    public static List<PartitionInput> convertToPartitionInputs(
            Collection<com.amazonaws.services.glue.model.Partition> parts) {
        List<PartitionInput> inputList = new ArrayList<>();

        for (com.amazonaws.services.glue.model.Partition part : parts) {
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            inputList.add(convertToPartitionInput(part));
        }
        return inputList;
    }

    public static UserDefinedFunctionInput convertToUserDefinedFunctionInput(Function hiveFunction) {
<<<<<<< HEAD
        UserDefinedFunctionInput functionInput = new UserDefinedFunctionInput();

        functionInput.setClassName(hiveFunction.getClassName());
        functionInput.setFunctionName(hiveFunction.getFunctionName());
        functionInput.setOwnerName(hiveFunction.getOwnerName());
        if (hiveFunction.getOwnerType() != null) {
            functionInput.setOwnerType(hiveFunction.getOwnerType().name());
        }
        functionInput.setResourceUris(HiveToCatalogConverter.covertResourceUriList(hiveFunction.getResourceUris()));
        return functionInput;
=======
        UserDefinedFunctionInput.Builder functionInput = UserDefinedFunctionInput.builder();

        functionInput.className(hiveFunction.getClassName());
        functionInput.functionName(hiveFunction.getFunctionName());
        functionInput.ownerName(hiveFunction.getOwnerName());
        if (hiveFunction.getOwnerType() != null) {
            functionInput.ownerType(hiveFunction.getOwnerType().name());
        }
        functionInput.resourceUris(HiveToCatalogConverter.covertResourceUriList(hiveFunction.getResourceUris()));
        return functionInput.build();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

}
