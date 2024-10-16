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

import com.starrocks.common.util.CompressionUtils;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.thrift.TUniqueId;
import org.junit.Test;

import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Tests for {@link StreamLoadInfo}. */
public class StreamLoadInfoTest {

    @Test
    public void testFromStreamLoad() throws Exception {
        TStreamLoadPutRequest request = buildTStreamLoadPutRequest();
        StreamLoadInfo info = StreamLoadInfo.fromTStreamLoadPutRequest(request, null);
        assertEquals(request.getTxnId(), info.getTxnId());
        assertEquals(request.getLoadId(), info.getId());
        assertEquals(request.getFileType(), info.getFileType());
        assertEquals(request.getFormatType(), info.getFormatType());
        assertEquals(request.getColumns(),
                    info.getColumnExprDescs().stream()
                        .map(ImportColumnDesc::getColumnName)
                            .collect(Collectors.joining(",")));
        assertEquals(request.getWhere(), AstToSQLBuilder.toSQL(info.getWhereExpr()));
        assertEquals(request.getPartitions(), String.join(",", info.getPartitions().getPartitionNames()));
        assertEquals(request.isIsTempPartition(), info.getPartitions().isTemp());
        assertEquals(request.isNegative(), info.getNegative());
        assertEquals(request.getTimeout(), info.getTimeout());
        assertEquals(request.isStrictMode(), info.isStrictMode());
        assertEquals(request.getTimezone(), info.getTimezone());
        assertEquals(request.getLoadMemLimit(), info.getLoadMemLimit());
        assertEquals(request.getJsonpaths(), info.getJsonPaths());
        assertEquals(request.getJson_root(), info.getJsonRoot());
        assertEquals(CompressionUtils.findTCompressionByName(request.getTransmission_compression_type()),
                info.getTransmisionCompressionType());
        assertEquals(request.getLoad_dop(), info.getLoadParallelRequestNum());
        assertEquals(request.isEnable_replicated_storage(), info.getEnableReplicatedStorage());
        assertEquals(request.getLog_rejected_record_num(), info.getLogRejectedRecordNum());
        assertEquals(request.isPartial_update(), info.isPartialUpdate());
        assertEquals(request.getPartial_update_mode(), info.getPartialUpdateMode());
        assertEquals(CompressionUtils.findTCompressionByName(request.getPayload_compression_type()),
                info.getPayloadCompressionType());
    }

    private TStreamLoadPutRequest buildTStreamLoadPutRequest() {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setTxnId(1);
        request.setLoadId(new TUniqueId(2, 3));
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_JSON);
        request.setColumns("c0,c1");
        request.setWhere("`c2` = 1");
        request.setPartitions("p0,p1");
        request.setIsTempPartition(true);
        request.setNegative(false);
        request.setTimeout(500);
        request.setStrictMode(true);
        request.setTimezone("Africa/Abidjan");
        request.setLoadMemLimit(1234567);
        request.setJsonpaths("[\\\"$.category\\\",\\\"$.price\\\",\\\"$.author\\\"]\"");
        request.setJson_root("$.RECORDS");
        request.setStrip_outer_array(true);
        request.setTransmission_compression_type("SNAPPY");
        request.setLoad_dop(2);
        request.setEnable_replicated_storage(true);
        request.setLog_rejected_record_num(10000);
        request.setPartial_update(true);
        request.setPartial_update_mode(TPartialUpdateMode.COLUMN_UPSERT_MODE);
        request.setPayload_compression_type("LZ4_FRAME");
        return request;
    }
}
