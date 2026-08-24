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
package com.starrocks.qe;

import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TBinaryEncodingFormat;
import com.starrocks.thrift.TBinaryEncodingLevel;
import com.starrocks.thrift.TQueryOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class SessionVariableTest {

    @Test
    public void testPaimonReaderMode() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        Assertions.assertEquals(SessionVariable.PaimonReaderMode.AUTO, sessionVariable.getPaimonReaderMode());

        Assertions.assertEquals("AUTO",
                VariableVarConverters.convert(SessionVariable.PAIMON_READER_MODE, "auto"));
        Assertions.assertEquals("JNI",
                VariableVarConverters.convert(SessionVariable.PAIMON_READER_MODE, "jNi"));
        String nativeMode = VariableVarConverters.convert(SessionVariable.PAIMON_READER_MODE, "native");
        Assertions.assertEquals("NATIVE", nativeMode);
        sessionVariable.setPaimonReaderMode(nativeMode);
        Assertions.assertEquals(SessionVariable.PaimonReaderMode.NATIVE, sessionVariable.getPaimonReaderMode());

        Assertions.assertThrows(DdlException.class,
                () -> VariableVarConverters.convert(SessionVariable.PAIMON_READER_MODE, "invalid"));
    }

    @Test
    public void testPaimonNativeReaderOptions() {
        SessionVariable sessionVariable = new SessionVariable();
        TQueryOptions options = sessionVariable.toThrift();
        Assertions.assertFalse(options.isPaimon_native_reader_enable_prefetch());
        Assertions.assertFalse(options.isPaimon_native_reader_enable_multi_thread_row_to_batch());
        Assertions.assertEquals(1, options.getPaimon_native_reader_row_to_batch_thread_num());
        Assertions.assertEquals(4L * 1024 * 1024, options.getPaimon_parquet_read_cache_hole_size_limit());
        Assertions.assertEquals(32L * 1024 * 1024, options.getPaimon_parquet_read_cache_range_size_limit());
        Assertions.assertEquals("coalesce",
                options.getPaimon_parquet_read_bitmap_row_range_refining_strategy());
        Assertions.assertEquals(32, options.getPaimon_parquet_read_bitmap_coalesce_hole_size_limit());

        sessionVariable.setPaimonNativeReaderEnablePrefetch(true);
        sessionVariable.setPaimonNativeReaderEnableMultiThreadRowToBatch(true);
        sessionVariable.setPaimonNativeReaderRowToBatchThreadNum(2);
        sessionVariable.setPaimonParquetReadCacheHoleSizeLimit(1024);
        sessionVariable.setPaimonParquetReadCacheRangeSizeLimit(8192);
        sessionVariable.setPaimonParquetReadBitmapRowRangeRefiningStrategy("trim");
        sessionVariable.setPaimonParquetReadBitmapCoalesceHoleSizeLimit(8);

        Assertions.assertTrue(sessionVariable.getPaimonNativeReaderEnablePrefetch());
        Assertions.assertTrue(sessionVariable.getPaimonNativeReaderEnableMultiThreadRowToBatch());
        Assertions.assertEquals(2, sessionVariable.getPaimonNativeReaderRowToBatchThreadNum());
        Assertions.assertEquals(1024, sessionVariable.getPaimonParquetReadCacheHoleSizeLimit());
        Assertions.assertEquals(8192, sessionVariable.getPaimonParquetReadCacheRangeSizeLimit());
        Assertions.assertEquals("trim", sessionVariable.getPaimonParquetReadBitmapRowRangeRefiningStrategy());
        Assertions.assertEquals(8, sessionVariable.getPaimonParquetReadBitmapCoalesceHoleSizeLimit());

        options = sessionVariable.toThrift();
        Assertions.assertTrue(options.isPaimon_native_reader_enable_prefetch());
        Assertions.assertTrue(options.isPaimon_native_reader_enable_multi_thread_row_to_batch());
        Assertions.assertEquals(2, options.getPaimon_native_reader_row_to_batch_thread_num());
        Assertions.assertEquals(1024, options.getPaimon_parquet_read_cache_hole_size_limit());
        Assertions.assertEquals(8192, options.getPaimon_parquet_read_cache_range_size_limit());
        Assertions.assertEquals("trim",
                options.getPaimon_parquet_read_bitmap_row_range_refining_strategy());
        Assertions.assertEquals(8, options.getPaimon_parquet_read_bitmap_coalesce_hole_size_limit());
    }

    @Test
    public void testPaimonIoOptionValidation() throws DdlException {
        Assertions.assertEquals("trim", VariableVarConverters.convert(
                SessionVariable.PAIMON_PARQUET_READ_BITMAP_ROW_RANGE_REFINING_STRATEGY, "TRIM"));
        Assertions.assertEquals("coalesce", VariableVarConverters.convert(
                SessionVariable.PAIMON_PARQUET_READ_BITMAP_ROW_RANGE_REFINING_STRATEGY, "Coalesce"));
        Assertions.assertThrows(DdlException.class, () -> VariableVarConverters.convert(
                SessionVariable.PAIMON_PARQUET_READ_BITMAP_ROW_RANGE_REFINING_STRATEGY, "invalid"));
        Assertions.assertThrows(DdlException.class, () -> VariableVarConverters.convert(
                SessionVariable.PAIMON_PARQUET_READ_CACHE_HOLE_SIZE_LIMIT, "-1"));
        Assertions.assertThrows(DdlException.class, () -> VariableVarConverters.convert(
                SessionVariable.PAIMON_PARQUET_READ_CACHE_RANGE_SIZE_LIMIT, "0"));
        Assertions.assertThrows(DdlException.class, () -> VariableVarConverters.convert(
                SessionVariable.PAIMON_PARQUET_READ_BITMAP_COALESCE_HOLE_SIZE_LIMIT, "-1"));
        Assertions.assertThrows(DdlException.class, () -> VariableVarConverters.convert(
                SessionVariable.PAIMON_PARQUET_READ_CACHE_HOLE_SIZE_LIMIT, "not-a-number"));
        Assertions.assertEquals("1024", VariableVarConverters.convert(
                SessionVariable.PAIMON_PARQUET_READ_CACHE_HOLE_SIZE_LIMIT, "1024"));
    }

    @Test
    public void testNonDefaultVariables() {
        SessionVariable sessionVariable = new SessionVariable();
        Map<String, SessionVariable.NonDefaultValue> nonDefaultVariables = sessionVariable.getNonDefaultVariables();
        Assertions.assertTrue(nonDefaultVariables.isEmpty());

        sessionVariable.setSqlDialect("test1");
        nonDefaultVariables = sessionVariable.getNonDefaultVariables();
        Assertions.assertEquals(1, nonDefaultVariables.size());
        Assertions.assertTrue(nonDefaultVariables.containsKey(SessionVariable.SQL_DIALECT));
        SessionVariable.NonDefaultValue kv = nonDefaultVariables.get(SessionVariable.SQL_DIALECT);
        Assertions.assertEquals(SessionVariable.DEFAULT_SESSION_VARIABLE.getSqlDialect(), kv.defaultValue);
        Assertions.assertEquals("test1", kv.actualValue);

        sessionVariable.setPipelineProfileLevel(100);
        nonDefaultVariables = sessionVariable.getNonDefaultVariables();
        Assertions.assertEquals(2, nonDefaultVariables.size());
        Assertions.assertTrue(nonDefaultVariables.containsKey(SessionVariable.PIPELINE_PROFILE_LEVEL));
        kv = nonDefaultVariables.get(SessionVariable.PIPELINE_PROFILE_LEVEL);
        Assertions.assertEquals(SessionVariable.DEFAULT_SESSION_VARIABLE.getPipelineProfileLevel(), kv.defaultValue);
        Assertions.assertEquals(100, kv.actualValue);
    }

    @Test
    public void testSetChooseMode() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setChooseExecuteInstancesMode("adaptive_increase");
        Assertions.assertTrue(sessionVariable.getChooseExecuteInstancesMode().enableIncreaseInstance());

        sessionVariable.setChooseExecuteInstancesMode("adaptive_decrease");
        Assertions.assertTrue(sessionVariable.getChooseExecuteInstancesMode().enableDecreaseInstance());

        sessionVariable.setChooseExecuteInstancesMode("auto");
        Assertions.assertTrue(sessionVariable.getChooseExecuteInstancesMode().enableIncreaseInstance());
        Assertions.assertTrue(sessionVariable.getChooseExecuteInstancesMode().enableDecreaseInstance());

        try {
            sessionVariable.setChooseExecuteInstancesMode("xxx");
            Assertions.fail("cannot set a invalid value");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Legal values of choose_execute_instances_mode are"),
                    e.getMessage());
        }
    }

    @Test
    public void testLakeBucketAssignMode() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setLakeBucketAssignMode("balance");
        Assertions.assertEquals(SessionVariableConstants.BALANCE, sessionVariable.getLakeBucketAssignMode());

        sessionVariable.setLakeBucketAssignMode("elastic");
        Assertions.assertEquals(SessionVariableConstants.ELASTIC, sessionVariable.getLakeBucketAssignMode());

        try {
            sessionVariable.setLakeBucketAssignMode("auto");
            Assertions.fail("cannot set a invalid value");
        } catch (Exception e) {
            Assertions.assertTrue(
                    e.getMessage().contains("Legal values of lake_bucket_assign_mode are elastic|balance"),
                    e.getMessage());
        }
    }

    @Test
    public void testSetEnableInsertPartialUpdate() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableInsertPartialUpdate(true);
        Assertions.assertTrue(sessionVariable.isEnableInsertPartialUpdate());

        sessionVariable.setEnableInsertPartialUpdate(false);
        Assertions.assertFalse(sessionVariable.isEnableInsertPartialUpdate());
    }

    @Test
    public void testConnectorSinkShuffleModeBackwardCompatibility() {
        SessionVariable sessionVariable = new SessionVariable();

        // Default mode is AUTO
        Assertions.assertEquals(com.starrocks.connector.ConnectorSinkShuffleMode.AUTO,
                sessionVariable.getIcebergConnectorSinkShuffleMode());

        // Backward compatibility: enableIcebergSinkGlobalShuffle implies FORCE when mode stays at default AUTO.
        com.starrocks.common.jmockit.Deencapsulation.setField(sessionVariable, "enableIcebergSinkGlobalShuffle", true);
        Assertions.assertEquals(com.starrocks.connector.ConnectorSinkShuffleMode.FORCE,
                sessionVariable.getIcebergConnectorSinkShuffleMode());

        // Explicitly set mode to NEVER should not be affected by legacy boolean.
        com.starrocks.common.jmockit.Deencapsulation.setField(sessionVariable, "connectorSinkShuffleMode", "never");
        Assertions.assertEquals(com.starrocks.connector.ConnectorSinkShuffleMode.NEVER,
                sessionVariable.getIcebergConnectorSinkShuffleMode());
    }

    @Test
    public void testEnableMVPlanner() {
        SessionVariable sessionVariable = new SessionVariable();

        // Test default value
        Assertions.assertFalse(sessionVariable.isMVPlanner());

        // Deprecated compatibility flag should remain inert.
        sessionVariable.setMVPlanner(true);
        Assertions.assertFalse(sessionVariable.isMVPlanner());

        sessionVariable.setMVPlanner(false);
        Assertions.assertFalse(sessionVariable.isMVPlanner());
    }

    @Test
    public void testEnableIncrementalRefreshMvIsNoOp() {
        SessionVariable sessionVariable = new SessionVariable();

        Assertions.assertFalse(sessionVariable.isEnableIncrementalRefreshMV());
        sessionVariable.setEnableIncrementalRefreshMv(true);
        Assertions.assertFalse(sessionVariable.isEnableIncrementalRefreshMV());
        sessionVariable.setEnableIncrementalRefreshMv(false);
        Assertions.assertFalse(sessionVariable.isEnableIncrementalRefreshMV());
    }

    @Test
    public void testBinaryEncodingDefaultsAndToThrift() {
        SessionVariable sessionVariable = new SessionVariable();
        TQueryOptions queryOptions = sessionVariable.toThrift();

        Assertions.assertEquals("hex", sessionVariable.getBinaryEncodingFormat());
        Assertions.assertEquals("nested", sessionVariable.getBinaryEncodingLevel());
        Assertions.assertEquals(TBinaryEncodingFormat.HEX, queryOptions.getBinary_encoding_format());
        Assertions.assertEquals(TBinaryEncodingLevel.NESTED, queryOptions.getBinary_encoding_level());
    }

    @Test
    public void testBinaryEncodingSettersNormalizeAndValidate() {
        SessionVariable sessionVariable = new SessionVariable();

        sessionVariable.setBinaryEncodingFormat("BASE64");
        sessionVariable.setBinaryEncodingLevel("ALL");
        TQueryOptions queryOptions = sessionVariable.toThrift();
        Assertions.assertEquals("base64", sessionVariable.getBinaryEncodingFormat());
        Assertions.assertEquals("all", sessionVariable.getBinaryEncodingLevel());
        Assertions.assertEquals(TBinaryEncodingFormat.BASE64, queryOptions.getBinary_encoding_format());
        Assertions.assertEquals(TBinaryEncodingLevel.ALL, queryOptions.getBinary_encoding_level());

        sessionVariable.setBinaryEncodingFormat(null);
        sessionVariable.setBinaryEncodingLevel(null);
        Assertions.assertEquals("hex", sessionVariable.getBinaryEncodingFormat());
        Assertions.assertEquals("nested", sessionVariable.getBinaryEncodingLevel());

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> sessionVariable.setBinaryEncodingFormat("invalid"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> sessionVariable.setBinaryEncodingLevel("invalid"));
    }

    @Test
    public void testLakeTabletInternalParallelSkewSplitRatioValidation() {
        SessionVariable sessionVariable = new SessionVariable();
        // A positive finite ratio is accepted.
        sessionVariable.setLakeTabletInternalParallelSkewSplitRatio(2.0);
        Assertions.assertEquals(2.0, sessionVariable.getLakeTabletInternalParallelSkewSplitRatio(), 0.0);
        // Non-positive or non-finite ratios are rejected: a non-positive value would make every sufficiently
        // large tablet look skewed (over-splitting), and NaN/Infinity would silently disable the skew override.
        Assertions.assertThrows(SemanticException.class,
                () -> sessionVariable.setLakeTabletInternalParallelSkewSplitRatio(0));
        Assertions.assertThrows(SemanticException.class,
                () -> sessionVariable.setLakeTabletInternalParallelSkewSplitRatio(-1.0));
        Assertions.assertThrows(SemanticException.class,
                () -> sessionVariable.setLakeTabletInternalParallelSkewSplitRatio(Double.NaN));
        Assertions.assertThrows(SemanticException.class,
                () -> sessionVariable.setLakeTabletInternalParallelSkewSplitRatio(Double.POSITIVE_INFINITY));
    }

    @Test
    public void testReplayFromJsonWithAlias() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();

        // alias key in JSON should be resolved
        sessionVariable.replayFromJson("{\"" +
                SessionVariable.SCAN_HIVE_PARTITION_NUM_LIMIT + "\": 1024}");
        Assertions.assertEquals(1024, sessionVariable.getScanLakePartitionNumLimit());

        // canonical name key should also work
        sessionVariable.replayFromJson("{\"" +
                SessionVariable.SCAN_LAKE_PARTITION_NUM_LIMIT + "\": 2048}");
        Assertions.assertEquals(2048, sessionVariable.getScanLakePartitionNumLimit());
    }

    @Test
    public void testReplayFromJsonNameTakesPriorityOverAlias() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();

        // when both name and alias are present, canonical name takes priority
        sessionVariable.replayFromJson("{\"" +
                SessionVariable.SCAN_LAKE_PARTITION_NUM_LIMIT + "\": 4096, \"" +
                SessionVariable.SCAN_HIVE_PARTITION_NUM_LIMIT + "\": 512}");
        Assertions.assertEquals(4096, sessionVariable.getScanLakePartitionNumLimit());
    }
}
