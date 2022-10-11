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

/**
 * This is the accessor interface for using AWS Glue as a metastore.
 * The generic AWSGlue interface{@link com.amazonaws.services.glue.AWSGlue}
 * has a number of methods that are irrelevant for clients using Glue only
 * as a metastore.
 * Think of this interface as a wrapper over AWSGlue. This additional layer
 * of abstraction achieves the following -
 * a) Hides the non-metastore related operations present in AWSGlue
 * b) Hides away the batching and pagination related limitations of AWSGlue
 */
public interface AWSGlueMetastore {

    void createDatabase(DatabaseInput databaseInput);

    Database getDatabase(String dbName);

    List<Database> getAllDatabases();

    void updateDatabase(String databaseName, DatabaseInput databaseInput);

    void deleteDatabase(String dbName);

    void createTable(String dbName, TableInput tableInput);

    Table getTable(String dbName, String tableName);

    List<Table> getTables(String dbname, String tablePattern);

    void updateTable(String dbName, TableInput tableInput);

    void deleteTable(String dbName, String tableName);

    Partition getPartition(String dbName, String tableName, List<String> partitionValues);

    List<Partition> getPartitionsByNames(String dbName, String tableName,
                                         List<PartitionValueList> partitionsToGet);

    List<Partition> getPartitions(String dbName, String tableName, String expression,
                                  long max) throws TException;

    void updatePartition(String dbName, String tableName, List<String> partitionValues,
                         PartitionInput partitionInput);

    void deletePartition(String dbName, String tableName, List<String> partitionValues);

    List<PartitionError> createPartitions(String dbName, String tableName,
                                          List<PartitionInput> partitionInputs);

    void createUserDefinedFunction(String dbName, UserDefinedFunctionInput functionInput);

    UserDefinedFunction getUserDefinedFunction(String dbName, String functionName);

    List<UserDefinedFunction> getUserDefinedFunctions(String dbName, String pattern);

    void deleteUserDefinedFunction(String dbName, String functionName);

    void updateUserDefinedFunction(String dbName, String functionName, UserDefinedFunctionInput functionInput);
}
