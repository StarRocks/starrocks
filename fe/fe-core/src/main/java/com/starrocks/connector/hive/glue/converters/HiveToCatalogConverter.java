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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveToCatalogConverter {

    public static software.amazon.awssdk.services.glue.model.Database convertDatabase(Database hiveDatabase) {
        software.amazon.awssdk.services.glue.model.Database.Builder catalogDatabase =
                software.amazon.awssdk.services.glue.model.Database.builder();
        catalogDatabase.name(hiveDatabase.getName());
        catalogDatabase.description(hiveDatabase.getDescription());
        catalogDatabase.locationUri(hiveDatabase.getLocationUri());
        catalogDatabase.parameters(hiveDatabase.getParameters());
        return catalogDatabase.build();
    }

    public static software.amazon.awssdk.services.glue.model.Table convertTable(
            Table hiveTable) {
        software.amazon.awssdk.services.glue.model.Table.Builder catalogTable =
                software.amazon.awssdk.services.glue.model.Table.builder();
        catalogTable.retention(hiveTable.getRetention());
        catalogTable.partitionKeys(convertFieldSchemaList(hiveTable.getPartitionKeys()));
        catalogTable.tableType(hiveTable.getTableType());
        catalogTable.name(hiveTable.getTableName());
        catalogTable.owner(hiveTable.getOwner());
        catalogTable.createTime(Instant.ofEpochSecond(hiveTable.getCreateTime()));
        catalogTable.lastAccessTime(Instant.ofEpochSecond(hiveTable.getLastAccessTime()));
        catalogTable.storageDescriptor(convertStorageDescriptor(hiveTable.getSd()));
        catalogTable.parameters(hiveTable.getParameters());
        catalogTable.viewExpandedText(hiveTable.getViewExpandedText());
        catalogTable.viewOriginalText(hiveTable.getViewOriginalText());

        return catalogTable.build();
    }

    public static software.amazon.awssdk.services.glue.model.StorageDescriptor convertStorageDescriptor(
            StorageDescriptor hiveSd) {
        software.amazon.awssdk.services.glue.model.StorageDescriptor.Builder catalogSd =
                software.amazon.awssdk.services.glue.model.StorageDescriptor.builder();
        catalogSd.numberOfBuckets(hiveSd.getNumBuckets());
        catalogSd.compressed(hiveSd.isCompressed());
        catalogSd.parameters(hiveSd.getParameters());
        catalogSd.bucketColumns(hiveSd.getBucketCols());
        catalogSd.columns(convertFieldSchemaList(hiveSd.getCols()));
        catalogSd.inputFormat(hiveSd.getInputFormat());
        catalogSd.location(hiveSd.getLocation());
        catalogSd.outputFormat(hiveSd.getOutputFormat());
        catalogSd.serdeInfo(convertSerDeInfo(hiveSd.getSerdeInfo()));
        catalogSd.skewedInfo(convertSkewedInfo(hiveSd.getSkewedInfo()));
        catalogSd.sortColumns(convertOrderList(hiveSd.getSortCols()));
        catalogSd.storedAsSubDirectories(hiveSd.isStoredAsSubDirectories());

        return catalogSd.build();
    }

    public static software.amazon.awssdk.services.glue.model.Column convertFieldSchema(
            FieldSchema hiveFieldSchema) {
        Column.Builder catalogFieldSchema = Column.builder();
        catalogFieldSchema.comment(hiveFieldSchema.getComment());
        catalogFieldSchema.name(hiveFieldSchema.getName());
        catalogFieldSchema.type(hiveFieldSchema.getType());

        return catalogFieldSchema.build();
    }

    public static List<software.amazon.awssdk.services.glue.model.Column> convertFieldSchemaList(
            List<FieldSchema> hiveFieldSchemaList) {
        List<software.amazon.awssdk.services.glue.model.Column> catalogFieldSchemaList =
                new ArrayList<software.amazon.awssdk.services.glue.model.Column>();
        for (FieldSchema hiveFs : hiveFieldSchemaList) {
            catalogFieldSchemaList.add(convertFieldSchema(hiveFs));
        }

        return catalogFieldSchemaList;
    }

    public static software.amazon.awssdk.services.glue.model.SerDeInfo convertSerDeInfo(
            SerDeInfo hiveSerDeInfo) {
        software.amazon.awssdk.services.glue.model.SerDeInfo.Builder catalogSerDeInfo =
                software.amazon.awssdk.services.glue.model.SerDeInfo.builder();
        catalogSerDeInfo.name(hiveSerDeInfo.getName());
        catalogSerDeInfo.parameters(hiveSerDeInfo.getParameters());
        catalogSerDeInfo.serializationLibrary(hiveSerDeInfo.getSerializationLib());

        return catalogSerDeInfo.build();
    }

    public static software.amazon.awssdk.services.glue.model.SkewedInfo convertSkewedInfo(SkewedInfo hiveSkewedInfo) {
        if (hiveSkewedInfo == null) {
            return null;
        }
        software.amazon.awssdk.services.glue.model.SkewedInfo.Builder catalogSkewedInfo =
                software.amazon.awssdk.services.glue.model.SkewedInfo.builder()
                        .skewedColumnNames(hiveSkewedInfo.getSkewedColNames())
                        .skewedColumnValues(convertSkewedValue(hiveSkewedInfo.getSkewedColValues()))
                        .skewedColumnValueLocationMaps(
                                convertSkewedMap(hiveSkewedInfo.getSkewedColValueLocationMaps()));
        return catalogSkewedInfo.build();
    }

    public static software.amazon.awssdk.services.glue.model.Order convertOrder(Order hiveOrder) {
        software.amazon.awssdk.services.glue.model.Order.Builder order =
                software.amazon.awssdk.services.glue.model.Order.builder();
        order.column(hiveOrder.getCol());
        order.sortOrder(hiveOrder.getOrder());

        return order.build();
    }

    public static List<software.amazon.awssdk.services.glue.model.Order> convertOrderList(List<Order> hiveOrderList) {
        if (hiveOrderList == null) {
            return null;
        }
        List<software.amazon.awssdk.services.glue.model.Order> catalogOrderList = new ArrayList<>();
        for (Order hiveOrder : hiveOrderList) {
            catalogOrderList.add(convertOrder(hiveOrder));
        }

        return catalogOrderList;
    }

    public static software.amazon.awssdk.services.glue.model.Partition convertPartition(Partition src) {
        software.amazon.awssdk.services.glue.model.Partition.Builder tgt =
                software.amazon.awssdk.services.glue.model.Partition.builder();

        tgt.databaseName(src.getDbName());
        tgt.tableName(src.getTableName());
        tgt.creationTime(Instant.ofEpochSecond(src.getCreateTime()));
        tgt.lastAccessTime(Instant.ofEpochSecond(src.getLastAccessTime()));
        tgt.parameters(src.getParameters());
        tgt.storageDescriptor(convertStorageDescriptor(src.getSd()));
        tgt.values(src.getValues());

        return tgt.build();
    }

    public static String convertListToString(final List<String> list) {
        if (list == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            String currentString = list.get(i);
            sb.append(currentString.length() + "$" + currentString);
        }

        return sb.toString();
    }

    public static Map<String, String> convertSkewedMap(final Map<List<String>, String> coreSkewedMap) {
        if (coreSkewedMap == null) {
            return null;
        }
        Map<String, String> catalogSkewedMap = new HashMap<>();
        for (List<String> coreKey : coreSkewedMap.keySet()) {
            catalogSkewedMap.put(convertListToString(coreKey), coreSkewedMap.get(coreKey));
        }
        return catalogSkewedMap;
    }

    public static List<String> convertSkewedValue(final List<List<String>> coreSkewedValue) {
        if (coreSkewedValue == null) {
            return null;
        }
        List<String> catalogSkewedValue = new ArrayList<>();
        for (int i = 0; i < coreSkewedValue.size(); i++) {
            catalogSkewedValue.add(convertListToString(coreSkewedValue.get(i)));
        }

        return catalogSkewedValue;
    }

    public static software.amazon.awssdk.services.glue.model.UserDefinedFunction convertFunction(
            final Function hiveFunction) {
        if (hiveFunction == null) {
            return null;
        }
        UserDefinedFunction.Builder catalogFunction =
                software.amazon.awssdk.services.glue.model.UserDefinedFunction.builder();
        catalogFunction.className(hiveFunction.getClassName());
        catalogFunction.functionName(hiveFunction.getFunctionName());
        catalogFunction.createTime(Instant.ofEpochSecond(hiveFunction.getCreateTime()));
        catalogFunction.ownerName(hiveFunction.getOwnerName());
        if (hiveFunction.getOwnerType() != null) {
            catalogFunction.ownerType(hiveFunction.getOwnerType().name());
        }
        catalogFunction.resourceUris(covertResourceUriList(hiveFunction.getResourceUris()));
        return catalogFunction.build();
    }

    public static List<software.amazon.awssdk.services.glue.model.ResourceUri> covertResourceUriList(
            final List<ResourceUri> hiveResourceUriList) {
        if (hiveResourceUriList == null) {
            return null;
        }
        List<software.amazon.awssdk.services.glue.model.ResourceUri> catalogResourceUriList = new ArrayList<>();
        for (ResourceUri hiveResourceUri : hiveResourceUriList) {
            software.amazon.awssdk.services.glue.model.ResourceUri.Builder catalogResourceUri =
                    software.amazon.awssdk.services.glue.model.ResourceUri.builder();
            catalogResourceUri.uri(hiveResourceUri.getUri());
            if (hiveResourceUri.getResourceType() != null) {
                catalogResourceUri.resourceType(hiveResourceUri.getResourceType().name());
            }
            catalogResourceUriList.add(catalogResourceUri.build());
        }
        return catalogResourceUriList;
    }

}