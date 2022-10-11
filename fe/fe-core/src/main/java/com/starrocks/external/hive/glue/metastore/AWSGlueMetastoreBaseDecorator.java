// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.glue.metastore;

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
import org.apache.thrift.TException;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class AWSGlueMetastoreBaseDecorator implements AWSGlueMetastore {

    private final AWSGlueMetastore awsGlueMetastore;

    public AWSGlueMetastoreBaseDecorator(AWSGlueMetastore awsGlueMetastore) {
        checkNotNull(awsGlueMetastore, "awsGlueMetastore can not be null");
        this.awsGlueMetastore = awsGlueMetastore;
    }

    @Override
    public void createDatabase(DatabaseInput databaseInput) {
        awsGlueMetastore.createDatabase(databaseInput);
    }

    @Override
    public Database getDatabase(String dbName) {
        return awsGlueMetastore.getDatabase(dbName);
    }

    @Override
    public List<Database> getAllDatabases() {
        return awsGlueMetastore.getAllDatabases();
    }

    @Override
    public void updateDatabase(String databaseName, DatabaseInput databaseInput) {
        awsGlueMetastore.updateDatabase(databaseName, databaseInput);
    }

    @Override
    public void deleteDatabase(String dbName) {
        awsGlueMetastore.deleteDatabase(dbName);
    }

    @Override
    public void createTable(String dbName, TableInput tableInput) {
        awsGlueMetastore.createTable(dbName, tableInput);
    }

    @Override
    public Table getTable(String dbName, String tableName) {
        return awsGlueMetastore.getTable(dbName, tableName);
    }

    @Override
    public List<Table> getTables(String dbname, String tablePattern) {
        return awsGlueMetastore.getTables(dbname, tablePattern);
    }

    @Override
    public void updateTable(String dbName, TableInput tableInput) {
        awsGlueMetastore.updateTable(dbName, tableInput);
    }

    @Override
    public void deleteTable(String dbName, String tableName) {
        awsGlueMetastore.deleteTable(dbName, tableName);
    }

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
        return awsGlueMetastore.getPartition(dbName, tableName, partitionValues);
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName, List<PartitionValueList> partitionsToGet) {
        return awsGlueMetastore.getPartitionsByNames(dbName, tableName, partitionsToGet);
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tableName, String expression, long max) throws TException {
        return awsGlueMetastore.getPartitions(dbName, tableName, expression, max);
    }

    @Override
    public void updatePartition(String dbName, String tableName, List<String> partitionValues, PartitionInput partitionInput) {
        awsGlueMetastore.updatePartition(dbName, tableName, partitionValues, partitionInput);
    }

    @Override
    public void deletePartition(String dbName, String tableName, List<String> partitionValues) {
        awsGlueMetastore.deletePartition(dbName, tableName, partitionValues);
    }

    @Override
    public List<PartitionError> createPartitions(String dbName, String tableName, List<PartitionInput> partitionInputs) {
        return awsGlueMetastore.createPartitions(dbName, tableName, partitionInputs);
    }

    @Override
    public void createUserDefinedFunction(String dbName, UserDefinedFunctionInput functionInput) {
        awsGlueMetastore.createUserDefinedFunction(dbName, functionInput);
    }

    @Override
    public UserDefinedFunction getUserDefinedFunction(String dbName, String functionName) {
        return awsGlueMetastore.getUserDefinedFunction(dbName, functionName);
    }

    @Override
    public List<UserDefinedFunction> getUserDefinedFunctions(String dbName, String pattern) {
        return awsGlueMetastore.getUserDefinedFunctions(dbName, pattern);
    }

    @Override
    public void deleteUserDefinedFunction(String dbName, String functionName) {
        awsGlueMetastore.deleteUserDefinedFunction(dbName, functionName);
    }

    @Override
    public void updateUserDefinedFunction(String dbName, String functionName, UserDefinedFunctionInput functionInput) {
        awsGlueMetastore.updateUserDefinedFunction(dbName, functionName, functionInput);
    }

}
