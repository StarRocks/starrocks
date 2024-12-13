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
import com.amazonaws.services.glue.model.ErrorDetail;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.connector.hive.glue.util.HiveTableValidator;
=======
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.connector.exception.StarRocksConnectorException;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
<<<<<<< HEAD

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

=======
import software.amazon.awssdk.services.glue.model.ErrorDetail;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.starrocks.connector.hive.HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.unified.UnifiedMetadata.isDeltaLakeTable;
import static com.starrocks.connector.unified.UnifiedMetadata.isIcebergTable;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

public class CatalogToHiveConverter {

    private static final Logger LOGGER = LogManager.getLogger(CatalogToHiveConverter.class);

    private static final ImmutableMap<String, HiveException> EXCEPTION_MAP =
            ImmutableMap.<String, HiveException>builder()
                    .put("AlreadyExistsException", new HiveException() {
                        public TException get(String msg) {
                            return new AlreadyExistsException(msg);
                        }
                    })
                    .put("InvalidInputException", new HiveException() {
                        public TException get(String msg) {
                            return new InvalidObjectException(msg);
                        }
                    })
                    .put("InternalServiceException", new HiveException() {
                        public TException get(String msg) {
                            return new MetaException(msg);
                        }
                    })
                    .put("ResourceNumberLimitExceededException", new HiveException() {
                        public TException get(String msg) {
                            return new MetaException(msg);
                        }
                    })
                    .put("OperationTimeoutException", new HiveException() {
                        public TException get(String msg) {
                            return new MetaException(msg);
                        }
                    })
                    .put("EntityNotFoundException", new HiveException() {
                        public TException get(String msg) {
                            return new NoSuchObjectException(msg);
                        }
                    })
                    .build();

    interface HiveException {
        TException get(String msg);
    }

    public static TException wrapInHiveException(Throwable e) {
        return getHiveException(e.getClass().getSimpleName(), e.getMessage());
    }

    public static TException errorDetailToHiveException(ErrorDetail errorDetail) {
<<<<<<< HEAD
        return getHiveException(errorDetail.getErrorCode(), errorDetail.getErrorMessage());
=======
        return getHiveException(errorDetail.errorCode(), errorDetail.errorMessage());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    private static TException getHiveException(String errorName, String errorMsg) {
        if (EXCEPTION_MAP.containsKey(errorName)) {
            return EXCEPTION_MAP.get(errorName).get(errorMsg);
        } else {
            LOGGER.warn("Hive Exception type not found for " + errorName);
            return new MetaException(errorMsg);
        }
    }

<<<<<<< HEAD
    public static Database convertDatabase(com.amazonaws.services.glue.model.Database catalogDatabase) {
        Database hiveDatabase = new Database();
        hiveDatabase.setName(catalogDatabase.getName());
        hiveDatabase.setDescription(catalogDatabase.getDescription());
        String location = catalogDatabase.getLocationUri();
        hiveDatabase.setLocationUri(location == null ? "" : location);
        hiveDatabase.setParameters(firstNonNull(catalogDatabase.getParameters(), Maps.<String, String>newHashMap()));
        return hiveDatabase;
    }

    public static FieldSchema convertFieldSchema(com.amazonaws.services.glue.model.Column catalogFieldSchema) {
        FieldSchema hiveFieldSchema = new FieldSchema();
        hiveFieldSchema.setType(catalogFieldSchema.getType());
        hiveFieldSchema.setName(catalogFieldSchema.getName());
        hiveFieldSchema.setComment(catalogFieldSchema.getComment());
=======
    public static Database convertDatabase(software.amazon.awssdk.services.glue.model.Database catalogDatabase) {
        Database hiveDatabase = new Database();
        hiveDatabase.setName(catalogDatabase.name());
        hiveDatabase.setDescription(catalogDatabase.description());
        String location = catalogDatabase.locationUri();
        hiveDatabase.setLocationUri(location == null ? "" : location);
        hiveDatabase.setParameters(firstNonNull(catalogDatabase.parameters(), Maps.<String, String>newHashMap()));
        return hiveDatabase;
    }

    public static FieldSchema convertFieldSchema(software.amazon.awssdk.services.glue.model.Column catalogFieldSchema) {
        FieldSchema hiveFieldSchema = new FieldSchema();
        hiveFieldSchema.setType(catalogFieldSchema.type());
        hiveFieldSchema.setName(catalogFieldSchema.name());
        hiveFieldSchema.setComment(catalogFieldSchema.comment());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        return hiveFieldSchema;
    }

    public static List<FieldSchema> convertFieldSchemaList(
<<<<<<< HEAD
            List<com.amazonaws.services.glue.model.Column> catalogFieldSchemaList) {
=======
            List<software.amazon.awssdk.services.glue.model.Column> catalogFieldSchemaList) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        List<FieldSchema> hiveFieldSchemaList = new ArrayList<>();
        if (catalogFieldSchemaList == null) {
            return hiveFieldSchemaList;
        }
<<<<<<< HEAD
        for (com.amazonaws.services.glue.model.Column catalogFieldSchema : catalogFieldSchemaList) {
=======
        for (software.amazon.awssdk.services.glue.model.Column catalogFieldSchema : catalogFieldSchemaList) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            hiveFieldSchemaList.add(convertFieldSchema(catalogFieldSchema));
        }

        return hiveFieldSchemaList;
    }

<<<<<<< HEAD
    public static Table convertTable(com.amazonaws.services.glue.model.Table catalogTable, String dbname) {
        Table hiveTable = new Table();
        hiveTable.setDbName(dbname);
        hiveTable.setTableName(catalogTable.getName());
        Date createTime = catalogTable.getCreateTime();
        hiveTable.setCreateTime(createTime == null ? 0 : (int) (createTime.getTime() / 1000));
        hiveTable.setOwner(catalogTable.getOwner());
        Date lastAccessedTime = catalogTable.getLastAccessTime();
        hiveTable.setLastAccessTime(lastAccessedTime == null ? 0 : (int) (lastAccessedTime.getTime() / 1000));
        hiveTable.setRetention(catalogTable.getRetention());
        // for iceberg table, don't need to set StorageDescriptor
        // just use metadata location in parameters that checked in HiveTableValidator
        // TODO(zombee0), check hudi deltalake
        if (!HiveTableValidator.isIcebergTable(catalogTable)) {
            hiveTable.setSd(convertStorageDescriptor(catalogTable.getStorageDescriptor()));
        }
        hiveTable.setPartitionKeys(convertFieldSchemaList(catalogTable.getPartitionKeys()));
        // Hive may throw a NPE during dropTable if the parameter map is null.
        Map<String, String> parameterMap = catalogTable.getParameters();
=======
    public static Table convertTable(software.amazon.awssdk.services.glue.model.Table catalogTable, String dbname) {
        Table hiveTable = new Table();
        hiveTable.setDbName(dbname);
        hiveTable.setTableName(catalogTable.name());
        Instant createTime = catalogTable.createTime();
        hiveTable.setCreateTime(createTime == null ? 0 : (int) (createTime.getEpochSecond()));
        hiveTable.setOwner(catalogTable.owner());
        Instant lastAccessedTime = catalogTable.lastAccessTime();
        hiveTable.setLastAccessTime(lastAccessedTime == null ? 0 : (int) (lastAccessedTime.getEpochSecond()));
        hiveTable.setRetention(catalogTable.retention());

        Optional<StorageDescriptor> optionalStorageDescriptor = Optional.ofNullable(catalogTable.storageDescriptor())
                .map(CatalogToHiveConverter::convertStorageDescriptor);
        // Provide dummy storage descriptor for compatibility if not explicitly configured
        if (!optionalStorageDescriptor.isPresent() && (isIcebergTable(catalogTable.parameters()) ||
                isDeltaLakeTable(catalogTable.parameters()))) {
            StorageDescriptor sd = new StorageDescriptor();
            sd.setCols(ImmutableList.of());
            sd.setInputFormat(MAPRED_PARQUET_INPUT_FORMAT_CLASS);
            optionalStorageDescriptor = Optional.of(sd);
        }

        if (!optionalStorageDescriptor.isPresent()) {
            throw new StarRocksConnectorException("Table StorageDescriptor is null for table " + catalogTable.name());
        }
        hiveTable.setSd(optionalStorageDescriptor.get());
        hiveTable.setPartitionKeys(convertFieldSchemaList(catalogTable.partitionKeys()));
        // Hive may throw a NPE during dropTable if the parameter map is null.
        Map<String, String> parameterMap = catalogTable.parameters();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (parameterMap == null) {
            parameterMap = Maps.newHashMap();
        }
        hiveTable.setParameters(parameterMap);
<<<<<<< HEAD
        hiveTable.setViewOriginalText(catalogTable.getViewOriginalText());
        hiveTable.setViewExpandedText(catalogTable.getViewExpandedText());
        hiveTable.setTableType(catalogTable.getTableType());
=======
        hiveTable.setViewOriginalText(catalogTable.viewOriginalText());
        hiveTable.setViewExpandedText(catalogTable.viewExpandedText());
        hiveTable.setTableType(catalogTable.tableType());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        return hiveTable;
    }

<<<<<<< HEAD
    public static TableMeta convertTableMeta(com.amazonaws.services.glue.model.Table catalogTable, String dbName) {
        TableMeta tableMeta = new TableMeta();
        tableMeta.setDbName(dbName);
        tableMeta.setTableName(catalogTable.getName());
        tableMeta.setTableType(catalogTable.getTableType());
        if (catalogTable.getParameters().containsKey("comment")) {
            tableMeta.setComments(catalogTable.getParameters().get("comment"));
=======
    public static TableMeta convertTableMeta(software.amazon.awssdk.services.glue.model.Table catalogTable,
                                             String dbName) {
        TableMeta tableMeta = new TableMeta();
        tableMeta.setDbName(dbName);
        tableMeta.setTableName(catalogTable.name());
        tableMeta.setTableType(catalogTable.tableType());
        if (catalogTable.parameters().containsKey("comment")) {
            tableMeta.setComments(catalogTable.parameters().get("comment"));
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
        return tableMeta;
    }

    public static StorageDescriptor convertStorageDescriptor(
<<<<<<< HEAD
            com.amazonaws.services.glue.model.StorageDescriptor catalogSd) {
        StorageDescriptor hiveSd = new StorageDescriptor();
        hiveSd.setCols(convertFieldSchemaList(catalogSd.getColumns()));
        hiveSd.setLocation(catalogSd.getLocation());
        hiveSd.setInputFormat(catalogSd.getInputFormat());
        hiveSd.setOutputFormat(catalogSd.getOutputFormat());
        hiveSd.setCompressed(catalogSd.getCompressed());
        hiveSd.setNumBuckets(catalogSd.getNumberOfBuckets());
        hiveSd.setSerdeInfo(catalogSd.getSerdeInfo() == null ? null : convertSerDeInfo(catalogSd.getSerdeInfo()));
        hiveSd.setBucketCols(firstNonNull(catalogSd.getBucketColumns(), Lists.<String>newArrayList()));
        hiveSd.setSortCols(convertOrderList(catalogSd.getSortColumns()));
        hiveSd.setParameters(firstNonNull(catalogSd.getParameters(), Maps.<String, String>newHashMap()));
        hiveSd.setSkewedInfo(catalogSd.getSkewedInfo() == null ? null : convertSkewedInfo(catalogSd.getSkewedInfo()));
        hiveSd.setStoredAsSubDirectories(catalogSd.getStoredAsSubDirectories());
=======
            software.amazon.awssdk.services.glue.model.StorageDescriptor catalogSd) {
        StorageDescriptor hiveSd = new StorageDescriptor();
        hiveSd.setCols(convertFieldSchemaList(catalogSd.columns()));
        hiveSd.setLocation(catalogSd.location());
        hiveSd.setInputFormat(catalogSd.inputFormat());
        hiveSd.setOutputFormat(catalogSd.outputFormat());
        hiveSd.setCompressed(catalogSd.compressed());
        hiveSd.setNumBuckets(catalogSd.numberOfBuckets());
        hiveSd.setSerdeInfo(catalogSd.serdeInfo() == null ? null : convertSerDeInfo(catalogSd.serdeInfo()));
        hiveSd.setBucketCols(firstNonNull(catalogSd.bucketColumns(), Lists.<String>newArrayList()));
        hiveSd.setSortCols(convertOrderList(catalogSd.sortColumns()));
        hiveSd.setParameters(firstNonNull(catalogSd.parameters(), Maps.<String, String>newHashMap()));
        hiveSd.setSkewedInfo(catalogSd.skewedInfo() == null ? null : convertSkewedInfo(catalogSd.skewedInfo()));
        hiveSd.setStoredAsSubDirectories(catalogSd.storedAsSubDirectories());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        return hiveSd;
    }

<<<<<<< HEAD
    public static Order convertOrder(com.amazonaws.services.glue.model.Order catalogOrder) {
        Order hiveOrder = new Order();
        hiveOrder.setCol(catalogOrder.getColumn());
        hiveOrder.setOrder(catalogOrder.getSortOrder());
=======
    public static Order convertOrder(software.amazon.awssdk.services.glue.model.Order catalogOrder) {
        Order hiveOrder = new Order();
        hiveOrder.setCol(catalogOrder.column());
        hiveOrder.setOrder(catalogOrder.sortOrder());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        return hiveOrder;
    }

<<<<<<< HEAD
    public static List<Order> convertOrderList(List<com.amazonaws.services.glue.model.Order> catalogOrderList) {
=======
    public static List<Order> convertOrderList(
            List<software.amazon.awssdk.services.glue.model.Order> catalogOrderList) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        List<Order> hiveOrderList = new ArrayList<>();
        if (catalogOrderList == null) {
            return hiveOrderList;
        }
<<<<<<< HEAD
        for (com.amazonaws.services.glue.model.Order catalogOrder : catalogOrderList) {
=======
        for (software.amazon.awssdk.services.glue.model.Order catalogOrder : catalogOrderList) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            hiveOrderList.add(convertOrder(catalogOrder));
        }

        return hiveOrderList;
    }

<<<<<<< HEAD
    public static SerDeInfo convertSerDeInfo(com.amazonaws.services.glue.model.SerDeInfo catalogSerDeInfo) {
        SerDeInfo hiveSerDeInfo = new SerDeInfo();
        hiveSerDeInfo.setName(catalogSerDeInfo.getName());
        hiveSerDeInfo.setParameters(firstNonNull(catalogSerDeInfo.getParameters(), Maps.<String, String>newHashMap()));
        hiveSerDeInfo.setSerializationLib(catalogSerDeInfo.getSerializationLibrary());
=======
    public static SerDeInfo convertSerDeInfo(software.amazon.awssdk.services.glue.model.SerDeInfo catalogSerDeInfo) {
        SerDeInfo hiveSerDeInfo = new SerDeInfo();
        hiveSerDeInfo.setName(catalogSerDeInfo.name());
        hiveSerDeInfo.setParameters(firstNonNull(catalogSerDeInfo.parameters(), Maps.<String, String>newHashMap()));
        hiveSerDeInfo.setSerializationLib(catalogSerDeInfo.serializationLibrary());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        return hiveSerDeInfo;
    }

<<<<<<< HEAD
    public static SkewedInfo convertSkewedInfo(com.amazonaws.services.glue.model.SkewedInfo catalogSkewedInfo) {
=======
    public static SkewedInfo convertSkewedInfo(
            software.amazon.awssdk.services.glue.model.SkewedInfo catalogSkewedInfo) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (catalogSkewedInfo == null) {
            return null;
        }

        SkewedInfo hiveSkewedInfo = new SkewedInfo();
        hiveSkewedInfo.setSkewedColNames(
<<<<<<< HEAD
                firstNonNull(catalogSkewedInfo.getSkewedColumnNames(), Lists.<String>newArrayList()));
        hiveSkewedInfo.setSkewedColValues(convertSkewedValue(catalogSkewedInfo.getSkewedColumnValues()));
        hiveSkewedInfo
                .setSkewedColValueLocationMaps(convertSkewedMap(catalogSkewedInfo.getSkewedColumnValueLocationMaps()));
        return hiveSkewedInfo;
    }

    public static Partition convertPartition(com.amazonaws.services.glue.model.Partition src) {
        Partition tgt = new Partition();
        Date createTime = src.getCreationTime();
        if (createTime != null) {
            tgt.setCreateTime((int) (createTime.getTime() / 1000));
=======
                firstNonNull(catalogSkewedInfo.skewedColumnNames(), Lists.<String>newArrayList()));
        hiveSkewedInfo.setSkewedColValues(convertSkewedValue(catalogSkewedInfo.skewedColumnValues()));
        hiveSkewedInfo
                .setSkewedColValueLocationMaps(convertSkewedMap(catalogSkewedInfo.skewedColumnValueLocationMaps()));
        return hiveSkewedInfo;
    }

    public static Partition convertPartition(software.amazon.awssdk.services.glue.model.Partition src) {
        Partition tgt = new Partition();
        Instant createTime = src.creationTime();
        if (createTime != null) {
            tgt.setCreateTime((int) (createTime.getEpochSecond()));
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            tgt.setCreateTimeIsSet(true);
        } else {
            tgt.setCreateTimeIsSet(false);
        }
<<<<<<< HEAD
        String dbName = src.getDatabaseName();
=======
        String dbName = src.databaseName();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (dbName != null) {
            tgt.setDbName(dbName);
            tgt.setDbNameIsSet(true);
        } else {
            tgt.setDbNameIsSet(false);
        }
<<<<<<< HEAD
        Date lastAccessTime = src.getLastAccessTime();
        if (lastAccessTime != null) {
            tgt.setLastAccessTime((int) (lastAccessTime.getTime() / 1000));
=======
        Instant lastAccessTime = src.lastAccessTime();
        if (lastAccessTime != null) {
            tgt.setLastAccessTime((int) (lastAccessTime.getEpochSecond()));
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            tgt.setLastAccessTimeIsSet(true);
        } else {
            tgt.setLastAccessTimeIsSet(false);
        }
<<<<<<< HEAD
        Map<String, String> params = src.getParameters();
=======
        Map<String, String> params = src.parameters();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        // A null parameter map causes Hive to throw a NPE
        // so ensure we do not return a Partition object with a null parameter map.
        if (params == null) {
            params = Maps.newHashMap();
        }

        tgt.setParameters(params);
        tgt.setParametersIsSet(true);

<<<<<<< HEAD
        String tableName = src.getTableName();
=======
        String tableName = src.tableName();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (tableName != null) {
            tgt.setTableName(tableName);
            tgt.setTableNameIsSet(true);
        } else {
            tgt.setTableNameIsSet(false);
        }

<<<<<<< HEAD
        List<String> values = src.getValues();
=======
        List<String> values = src.values();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (values != null) {
            tgt.setValues(values);
            tgt.setValuesIsSet(true);
        } else {
            tgt.setValuesIsSet(false);
        }

<<<<<<< HEAD
        com.amazonaws.services.glue.model.StorageDescriptor sd = src.getStorageDescriptor();
=======
        software.amazon.awssdk.services.glue.model.StorageDescriptor sd = src.storageDescriptor();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (sd != null) {
            StorageDescriptor hiveSd = convertStorageDescriptor(sd);
            tgt.setSd(hiveSd);
            tgt.setSdIsSet(true);
        } else {
            tgt.setSdIsSet(false);
        }

        return tgt;
    }

<<<<<<< HEAD
    public static List<Partition> convertPartitions(List<com.amazonaws.services.glue.model.Partition> src) {
=======
    public static List<Partition> convertPartitions(List<software.amazon.awssdk.services.glue.model.Partition> src) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (src == null) {
            return null;
        }

        List<Partition> target = Lists.newArrayList();
<<<<<<< HEAD
        for (com.amazonaws.services.glue.model.Partition partition : src) {
=======
        for (software.amazon.awssdk.services.glue.model.Partition partition : src) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            target.add(convertPartition(partition));
        }
        return target;
    }

    public static List<String> convertStringToList(final String s) {
        if (s == null) {
            return null;
        }
        List<String> listString = new ArrayList<>();
        for (int i = 0; i < s.length(); ) {
            StringBuilder length = new StringBuilder();
            for (int j = i; j < s.length(); j++) {
                if (s.charAt(j) != '$') {
                    length.append(s.charAt(j));
                } else {
                    int lengthOfString = Integer.valueOf(length.toString());
                    listString.add(s.substring(j + 1, j + 1 + lengthOfString));
                    i = j + 1 + lengthOfString;
                    break;
                }
            }
        }
        return listString;
    }

    @Nonnull
    public static Map<List<String>, String> convertSkewedMap(final @Nullable Map<String, String> catalogSkewedMap) {
        Map<List<String>, String> skewedMap = new HashMap<>();
        if (catalogSkewedMap == null) {
            return skewedMap;
        }

        for (String coralKey : catalogSkewedMap.keySet()) {
            skewedMap.put(convertStringToList(coralKey), catalogSkewedMap.get(coralKey));
        }
        return skewedMap;
    }

    @Nonnull
    public static List<List<String>> convertSkewedValue(final @Nullable List<String> catalogSkewedValue) {
        List<List<String>> skewedValues = new ArrayList<>();
        if (catalogSkewedValue == null) {
            return skewedValues;
        }

        for (String skewValue : catalogSkewedValue) {
            skewedValues.add(convertStringToList(skewValue));
        }
        return skewedValues;
    }

    public static PrincipalType convertPrincipalType(
<<<<<<< HEAD
            com.amazonaws.services.glue.model.PrincipalType catalogPrincipalType) {
=======
            software.amazon.awssdk.services.glue.model.PrincipalType catalogPrincipalType) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (catalogPrincipalType == null) {
            return null;
        }

<<<<<<< HEAD
        if (catalogPrincipalType == com.amazonaws.services.glue.model.PrincipalType.GROUP) {
            return PrincipalType.GROUP;
        } else if (catalogPrincipalType == com.amazonaws.services.glue.model.PrincipalType.USER) {
            return PrincipalType.USER;
        } else if (catalogPrincipalType == com.amazonaws.services.glue.model.PrincipalType.ROLE) {
=======
        if (catalogPrincipalType == software.amazon.awssdk.services.glue.model.PrincipalType.GROUP) {
            return PrincipalType.GROUP;
        } else if (catalogPrincipalType == software.amazon.awssdk.services.glue.model.PrincipalType.USER) {
            return PrincipalType.USER;
        } else if (catalogPrincipalType == software.amazon.awssdk.services.glue.model.PrincipalType.ROLE) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            return PrincipalType.ROLE;
        }
        throw new RuntimeException("Unknown principal type:" + catalogPrincipalType.name());
    }

    public static Function convertFunction(final String dbName,
<<<<<<< HEAD
                                           final com.amazonaws.services.glue.model.UserDefinedFunction catalogFunction) {
=======
                                           final software.amazon.awssdk.services.glue.model.UserDefinedFunction catalogFunction) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (catalogFunction == null) {
            return null;
        }
        Function hiveFunction = new Function();
<<<<<<< HEAD
        hiveFunction.setClassName(catalogFunction.getClassName());
        hiveFunction.setCreateTime((int) (catalogFunction.getCreateTime().getTime() / 1000));
        hiveFunction.setDbName(dbName);
        hiveFunction.setFunctionName(catalogFunction.getFunctionName());
        hiveFunction.setFunctionType(FunctionType.JAVA);
        hiveFunction.setOwnerName(catalogFunction.getOwnerName());
        hiveFunction.setOwnerType(convertPrincipalType(
                com.amazonaws.services.glue.model.PrincipalType.fromValue(catalogFunction.getOwnerType())));
        hiveFunction.setResourceUris(convertResourceUriList(catalogFunction.getResourceUris()));
=======
        hiveFunction.setClassName(catalogFunction.className());
        hiveFunction.setCreateTime((int) (catalogFunction.createTime().getEpochSecond()));
        hiveFunction.setDbName(dbName);
        hiveFunction.setFunctionName(catalogFunction.functionName());
        hiveFunction.setFunctionType(FunctionType.JAVA);
        hiveFunction.setOwnerName(catalogFunction.ownerName());
        hiveFunction.setOwnerType(convertPrincipalType(catalogFunction.ownerType()));
        hiveFunction.setResourceUris(convertResourceUriList(catalogFunction.resourceUris()));
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return hiveFunction;
    }

    public static List<ResourceUri> convertResourceUriList(
<<<<<<< HEAD
            final List<com.amazonaws.services.glue.model.ResourceUri> catalogResourceUriList) {
=======
            final List<software.amazon.awssdk.services.glue.model.ResourceUri> catalogResourceUriList) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (catalogResourceUriList == null) {
            return null;
        }
        List<ResourceUri> hiveResourceUriList = new ArrayList<>();
<<<<<<< HEAD
        for (com.amazonaws.services.glue.model.ResourceUri catalogResourceUri : catalogResourceUriList) {
            ResourceUri hiveResourceUri = new ResourceUri();
            hiveResourceUri.setUri(catalogResourceUri.getUri());
            if (catalogResourceUri.getResourceType() != null) {
                hiveResourceUri.setResourceType(ResourceType.valueOf(catalogResourceUri.getResourceType()));
=======
        for (software.amazon.awssdk.services.glue.model.ResourceUri catalogResourceUri : catalogResourceUriList) {
            ResourceUri hiveResourceUri = new ResourceUri();
            hiveResourceUri.setUri(catalogResourceUri.uri());
            if (catalogResourceUri.resourceType() != null) {
                hiveResourceUri.setResourceType(ResourceType.valueOf(catalogResourceUri.resourceType().toString()));
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            }
            hiveResourceUriList.add(hiveResourceUri);
        }

        return hiveResourceUriList;
    }

}