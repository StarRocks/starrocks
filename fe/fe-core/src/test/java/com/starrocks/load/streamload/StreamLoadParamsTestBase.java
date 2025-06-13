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

package com.starrocks.load.streamload;

import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TPartialUpdateMode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** Test base for {@link StreamLoadParams}. */
public abstract class StreamLoadParamsTestBase {

    @Test
    public void testFileFormatType() {
        assertFalse(buildFileFormatType(null).getFileFormatType().isPresent());
        assertEquals(TFileFormatType.FORMAT_CSV_PLAIN,
                buildFileFormatType(TFileFormatType.FORMAT_CSV_PLAIN).getFileFormatType().orElse(null));
        assertEquals(TFileFormatType.FORMAT_JSON,
                buildFileFormatType(TFileFormatType.FORMAT_JSON).getFileFormatType().orElse(null));
        assertEquals(TFileFormatType.FORMAT_CSV_GZ,
                buildFileFormatType(TFileFormatType.FORMAT_CSV_GZ).getFileFormatType().orElse(null));
        assertEquals(TFileFormatType.FORMAT_CSV_BZ2,
                buildFileFormatType(TFileFormatType.FORMAT_CSV_BZ2).getFileFormatType().orElse(null));
        assertEquals(TFileFormatType.FORMAT_CSV_LZ4_FRAME,
                buildFileFormatType(TFileFormatType.FORMAT_CSV_LZ4_FRAME).getFileFormatType().orElse(null));
        assertEquals(TFileFormatType.FORMAT_CSV_DEFLATE,
                buildFileFormatType(TFileFormatType.FORMAT_CSV_DEFLATE).getFileFormatType().orElse(null));
        assertEquals(TFileFormatType.FORMAT_CSV_DEFLATE,
                buildFileFormatType(TFileFormatType.FORMAT_CSV_DEFLATE).getFileFormatType().orElse(null));
        assertEquals(TFileFormatType.FORMAT_CSV_ZSTD,
                buildFileFormatType(TFileFormatType.FORMAT_CSV_ZSTD).getFileFormatType().orElse(null));
    }

    protected abstract StreamLoadParams buildFileFormatType(TFileFormatType expected);

    @Test
    public void testFileType() {
        assertFalse(buildFileType(null).getFileType().isPresent());
        assertEquals(TFileType.FILE_STREAM, buildFileType(TFileType.FILE_STREAM).getFileType().orElse(null));
    }

    protected abstract StreamLoadParams buildFileType(TFileType expected);

    @Test
    public void testFilePath() {
        assertFalse(buildFilePath(null).getFilePath().isPresent());
    }

    protected abstract StreamLoadParams buildFilePath(String expected);

    @Test
    public void testColumns() {
        assertFalse(buildColumns(null).getColumns().isPresent());
        assertEquals("c0,c1,c2", buildColumns("c0,c1,c2").getColumns().orElse(null));
    }

    protected abstract StreamLoadParams buildColumns(String expected);

    @Test
    public void testWhere() {
        assertFalse(buildWhere(null).getWhere().isPresent());
        assertEquals("c0=123456", buildWhere("c0=123456").getWhere().orElse(null));
    }

    protected abstract StreamLoadParams buildWhere(String expected);

    @Test
    public void testPartitions() {
        assertFalse(buildPartitions(null).getPartitions().isPresent());
        assertEquals("p1,p2", buildPartitions("p1,p2").getPartitions().orElse(null));
    }

    protected abstract StreamLoadParams buildPartitions(String expected);

    @Test
    public void testIsTempPartition() {
        assertFalse(buildIsTempPartition(null).getIsTempPartition().isPresent());
        assertEquals(true, buildIsTempPartition(true).getIsTempPartition().orElse(null));
        assertEquals(false, buildIsTempPartition(false).getIsTempPartition().orElse(null));
    }

    protected abstract StreamLoadParams buildIsTempPartition(Boolean expected);

    @Test
    public void testNegative() {
        assertFalse(buildNegative(null).getNegative().isPresent());
        assertEquals(true, buildNegative(true).getNegative().orElse(null));
        assertEquals(false, buildNegative(false).getNegative().orElse(null));
    }

    protected abstract StreamLoadParams buildNegative(Boolean expected);

    @Test
    public void testMaxFilterRatio() {
        assertFalse(buildMaxFilterRatio(null).getMaxFilterRatio().isPresent());
        assertEquals(0.0, buildMaxFilterRatio(0.0).getMaxFilterRatio().get(), 0.0);
        assertEquals(0.5, buildMaxFilterRatio(0.5).getMaxFilterRatio().get(), 0.0);
        assertEquals(0.9, buildMaxFilterRatio(0.9).getMaxFilterRatio().get(), 0.0);
        assertEquals(1.0, buildMaxFilterRatio(1.0).getMaxFilterRatio().get(), 0.0);
    }

    protected abstract StreamLoadParams buildMaxFilterRatio(Double expected);

    @Test
    public void testTimeout() {
        assertFalse(buildTimeout(null).getTimeout().isPresent());
        assertEquals(0, (int) buildTimeout(0).getTimeout().get());
        assertEquals(100, (int) buildTimeout(100).getTimeout().get());
    }

    protected abstract StreamLoadParams buildTimeout(Integer expected);

    @Test
    public void testStrictMode() {
        assertFalse(buildStrictMode(null).getStrictMode().isPresent());
        assertEquals(true, buildStrictMode(true).getStrictMode().orElse(null));
        assertEquals(false, buildStrictMode(false).getStrictMode().orElse(null));
    }

    protected abstract StreamLoadParams buildStrictMode(Boolean expected);

    @Test
    public void testTimezone() {
        assertFalse(buildTimezone(null).getTimezone().isPresent());
        assertEquals("Africa/Abidjan", buildTimezone("Africa/Abidjan").getTimezone().orElse(null));
    }

    protected abstract StreamLoadParams buildTimezone(String expected);

    @Test
    public void testLoadMemLimit() {
        assertFalse(buildLoadMemLimit(null).getLoadMemLimit().isPresent());
        assertEquals(123434, (long) buildLoadMemLimit(123434L).getLoadMemLimit().get());
        assertEquals(847423, (long) buildLoadMemLimit(847423L).getLoadMemLimit().get());
    }

    protected abstract StreamLoadParams buildLoadMemLimit(Long expected);

    @Test
    public void testTransmissionCompressionType() {
        assertFalse(buildTransmissionCompressionType(null).getTransmissionCompressionType().isPresent());
        assertEquals("LZ4",
                buildTransmissionCompressionType("LZ4").getTransmissionCompressionType().orElse(null));
        assertEquals("SNAPPY",
                buildTransmissionCompressionType("SNAPPY").getTransmissionCompressionType().orElse(null));
    }

    protected abstract StreamLoadParams buildTransmissionCompressionType(String expected);

    @Test
    public void testLoadDop() {
        assertFalse(buildLoadDop(null).getLoadDop().isPresent());
        assertEquals(1, (int) buildLoadDop(1).getLoadDop().get());
        assertEquals(10, (int) buildLoadDop(10).getLoadDop().get());
    }

    protected abstract StreamLoadParams buildLoadDop(Integer expected);

    @Test
    public void testEnableReplicatedStorage() {
        assertFalse(buildEnableReplicatedStorage(null).getEnableReplicatedStorage().isPresent());
        assertEquals(true, buildEnableReplicatedStorage(true).getEnableReplicatedStorage().orElse(null));
        assertEquals(false, buildEnableReplicatedStorage(false).getEnableReplicatedStorage().orElse(null));
    }

    protected abstract StreamLoadParams buildEnableReplicatedStorage(Boolean expected);

    @Test
    public void testMergeCondition() {
        assertFalse(buildMergeCondition(null).getMergeCondition().isPresent());
        assertEquals("c10", buildMergeCondition("c10").getMergeCondition().orElse(null));
    }

    protected abstract StreamLoadParams buildMergeCondition(String expected);

    @Test
    public void testLogRejectedRecordNum() {
        assertFalse(buildLogRejectedRecordNum(null).getLogRejectedRecordNum().isPresent());
        assertEquals(12, (long) buildLogRejectedRecordNum(12L).getLogRejectedRecordNum().get());
        assertEquals(1002, (long) buildLogRejectedRecordNum(1002L).getLogRejectedRecordNum().get());
    }

    protected abstract StreamLoadParams buildLogRejectedRecordNum(Long expected);

    @Test
    public void testPartialUpdate() {
        assertFalse(buildPartialUpdate(null).getPartialUpdate().isPresent());
        assertEquals(true, buildPartialUpdate(true).getPartialUpdate().orElse(null));
        assertEquals(false, buildPartialUpdate(false).getPartialUpdate().orElse(null));
    }

    protected abstract StreamLoadParams buildPartialUpdate(Boolean expected);

    @Test
    public void testPartialUpdateMode() {
        assertFalse(buildPartialUpdateMode(null).getPartialUpdateMode().isPresent());
        assertEquals(TPartialUpdateMode.ROW_MODE,
                buildPartialUpdateMode(TPartialUpdateMode.ROW_MODE).getPartialUpdateMode().orElse(null));
        assertEquals(TPartialUpdateMode.COLUMN_UPSERT_MODE,
                buildPartialUpdateMode(TPartialUpdateMode.COLUMN_UPSERT_MODE).getPartialUpdateMode().orElse(null));
        assertEquals(TPartialUpdateMode.AUTO_MODE,
                buildPartialUpdateMode(TPartialUpdateMode.AUTO_MODE).getPartialUpdateMode().orElse(null));
    }

    protected abstract StreamLoadParams buildPartialUpdateMode(TPartialUpdateMode expected);

    @Test
    public void testPayloadCompressionType() {
        assertFalse(buildPayloadCompressionType(null).getPayloadCompressionType().isPresent());
        assertEquals("LZ4",
                buildPayloadCompressionType("LZ4").getPayloadCompressionType().orElse(null));
        assertEquals("SNAPPY",
                buildPayloadCompressionType("SNAPPY").getPayloadCompressionType().orElse(null));
    }

    protected abstract StreamLoadParams buildPayloadCompressionType(String expected);

    @Test
    public void testWarehouse() {
        assertFalse(buildWarehouse(null).getWarehouse().isPresent());
        assertEquals("ingestion", buildWarehouse("ingestion").getWarehouse().orElse(null));
    }

    protected abstract StreamLoadParams buildWarehouse(String expected);

    @Test
    public void testColumnSeparator() {
        assertFalse(buildColumnSeparator(null).getColumnSeparator().isPresent());
        assertEquals(",", buildColumnSeparator(",").getColumnSeparator().orElse(null));
    }

    protected abstract StreamLoadParams buildColumnSeparator(String expected);

    @Test
    public void testRowDelimiter() {
        assertFalse(buildRowDelimiter(null).getRowDelimiter().isPresent());
        assertEquals("|", buildRowDelimiter("|").getRowDelimiter().orElse(null));
    }

    protected abstract StreamLoadParams buildRowDelimiter(String expected);

    @Test
    public void testSkipHeader() {
        assertFalse(buildSkipHeader(null).getSkipHeader().isPresent());
        assertEquals(200, (long) buildSkipHeader(200L).getSkipHeader().get());
    }

    protected abstract StreamLoadParams buildSkipHeader(Long expected);

    @Test
    public void testEnclose() {
        assertFalse(buildEnclose(null).getEnclose().isPresent());
        assertEquals((byte) 0x01, (byte) buildEnclose((byte) 0x01).getEnclose().get());
    }

    protected abstract StreamLoadParams buildEnclose(Byte expected);

    @Test
    public void testEscape() {
        assertFalse(buildEscape(null).getEscape().isPresent());
        assertEquals((byte) 0x02, (byte) buildEscape((byte) 0x02).getEscape().get());
    }

    protected abstract StreamLoadParams buildEscape(Byte expected);

    @Test
    public void testTrimSpace() {
        assertFalse(buildTrimSpace(null).getTrimSpace().isPresent());
        assertEquals(true, buildTrimSpace(true).getTrimSpace().orElse(null));
        assertEquals(false, buildTrimSpace(false).getTrimSpace().orElse(null));
    }

    protected abstract StreamLoadParams buildTrimSpace(Boolean expected);

    @Test
    public void testJsonPaths() {
        assertFalse(buildJsonPaths(null).getJsonPaths().isPresent());
        assertEquals("[\\\"$.category\\\",\\\"$.price\\\",\\\"$.author\\\"]\"",
                buildJsonPaths("[\\\"$.category\\\",\\\"$.price\\\",\\\"$.author\\\"]\"").getJsonPaths().orElse(null));
    }

    protected abstract StreamLoadParams buildJsonPaths(String expected);

    @Test
    public void testJsonRoot() {
        assertFalse(buildJsonRoot(null).getJsonRoot().isPresent());
        assertEquals("$.RECORDS", buildJsonRoot("$.RECORDS").getJsonRoot().orElse(null));
    }

    protected abstract StreamLoadParams buildJsonRoot(String expected);

    @Test
    public void testStripOuterArray() {
        assertFalse(buildStripOuterArray(null).getStripOuterArray().isPresent());
        assertEquals(true, buildStripOuterArray(true).getStripOuterArray().orElse(null));
        assertEquals(false, buildStripOuterArray(false).getStripOuterArray().orElse(null));
    }

    protected abstract StreamLoadParams buildStripOuterArray(Boolean expected);
}
