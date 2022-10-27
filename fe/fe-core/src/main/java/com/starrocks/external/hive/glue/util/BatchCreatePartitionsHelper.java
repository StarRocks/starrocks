// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.glue.util;

import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.google.common.collect.Lists;
import com.starrocks.external.hive.glue.converters.CatalogToHiveConverter;
import com.starrocks.external.hive.glue.converters.GlueInputConverter;
import com.starrocks.external.hive.glue.metastore.AWSGlueMetastore;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.starrocks.external.hive.glue.util.PartitionUtils.isInvalidUserInputException;

public final class BatchCreatePartitionsHelper {

    private static final Logger LOGGER = Logger.getLogger(BatchCreatePartitionsHelper.class);

    private final AWSGlueMetastore glueClient;
    private final String databaseName;
    private final String tableName;
    private final List<Partition> partitions;
    private final boolean ifNotExists;
    private Map<PartitionKey, Partition> partitionMap;
    private List<Partition> partitionsFailed;
    private TException firstTException;
    private String catalogId;

    public BatchCreatePartitionsHelper(AWSGlueMetastore glueClient, String databaseName, String tableName,
                                       String catalogId,
                                       List<Partition> partitions, boolean ifNotExists) {
        this.glueClient = glueClient;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.catalogId = catalogId;
        this.partitions = partitions;
        this.ifNotExists = ifNotExists;
    }

    public BatchCreatePartitionsHelper createPartitions() {
        partitionMap = PartitionUtils.buildPartitionMap(partitions);
        partitionsFailed = Lists.newArrayList();

        try {
            List<PartitionError> result =
                    glueClient.createPartitions(databaseName, tableName,
                            GlueInputConverter.convertToPartitionInputs(partitionMap.values()));
            processResult(result);
        } catch (Exception e) {
            LOGGER.error("Exception thrown while creating partitions in DataCatalog: ", e);
            firstTException = CatalogToHiveConverter.wrapInHiveException(e);
            if (isInvalidUserInputException(e)) {
                setAllFailed();
            } else {
                checkIfPartitionsCreated();
            }
        }
        return this;
    }

    private void setAllFailed() {
        partitionsFailed = partitions;
        partitionMap.clear();
    }

    private void processResult(List<PartitionError> partitionErrors) {
        if (partitionErrors == null || partitionErrors.isEmpty()) {
            return;
        }

        LOGGER.error(String.format("BatchCreatePartitions failed to create %d out of %d partitions. \n",
                partitionErrors.size(), partitionMap.size()));

        for (PartitionError partitionError : partitionErrors) {
            Partition partitionFailed = partitionMap.remove(new PartitionKey(partitionError.getPartitionValues()));

            TException exception = CatalogToHiveConverter.errorDetailToHiveException(partitionError.getErrorDetail());
            if (ifNotExists && exception instanceof AlreadyExistsException) {
                // AlreadyExistsException is allowed, so we shouldn't add the partition to partitionsFailed list
                continue;
            }
            LOGGER.error(exception);
            if (firstTException == null) {
                firstTException = exception;
            }
            partitionsFailed.add(partitionFailed);
        }
    }

    private void checkIfPartitionsCreated() {
        for (Partition partition : partitions) {
            if (!partitionExists(partition)) {
                partitionsFailed.add(partition);
                partitionMap.remove(new PartitionKey(partition));
            }
        }
    }

    private boolean partitionExists(Partition partition) {

        try {
            Partition partitionReturned = glueClient.getPartition(databaseName, tableName, partition.getValues());
            return partitionReturned != null; //probably always true here
        } catch (EntityNotFoundException e) {
            // here we assume namespace and table exist. It is assured by calling "isInvalidUserInputException" method above
            return false;
        } catch (Exception e) {
            LOGGER.error(String.format("Get partition request %s failed. ",
                    StringUtils.join(partition.getValues(), "/")), e);
            // partition status unknown, we assume that the partition was not created
            return false;
        }
    }

    public TException getFirstTException() {
        return firstTException;
    }

    public Collection<Partition> getPartitionsCreated() {
        return partitionMap.values();
    }

    public List<Partition> getPartitionsFailed() {
        return partitionsFailed;
    }

}
