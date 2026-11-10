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

package com.starrocks.rpc;

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.starrocks.proto.AIExecutionStatisticsPB;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.StatusPB;
import com.starrocks.thrift.TAIExecutionStatistics;
import com.starrocks.thrift.TAuditStatistics;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AIExecutionStatisticsSerializationTest {
    @Test
    public void testNestedJProtobufRoundTrip() throws Exception {
        AIExecutionStatisticsPB ai = new AIExecutionStatisticsPB();
        ai.taskCount = 1L;
        ai.requestCount = 2L;
        ai.retryCount = 3L;
        ai.timeoutCount = 4L;
        ai.errorCount = 5L;
        ai.httpTimeNs = 6L;
        ai.promptTokens = 7L;
        ai.completionTokens = 8L;
        ai.totalTokens = 9L;
        ai.promptUsageCount = 10L;
        ai.completionUsageCount = 11L;
        ai.totalUsageCount = 12L;
        PFetchDataResult result = new PFetchDataResult();
        result.status = new StatusPB();
        result.status.statusCode = 0;
        result.queryStatistics = new PQueryStatistics();
        result.queryStatistics.aiStatistics = ai;
        Codec<PFetchDataResult> codec = ProtobufProxy.create(PFetchDataResult.class);
        AIExecutionStatisticsPB decoded = codec.decode(codec.encode(result)).queryStatistics.aiStatistics;
        Assertions.assertNotNull(decoded);
        Assertions.assertArrayEquals(new Long[] {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L},
                new Long[] {decoded.taskCount, decoded.requestCount, decoded.retryCount, decoded.timeoutCount,
                        decoded.errorCount, decoded.httpTimeNs, decoded.promptTokens, decoded.completionTokens,
                        decoded.totalTokens, decoded.promptUsageCount, decoded.completionUsageCount, decoded.totalUsageCount});
    }

    @Test
    public void testJProtobufPreservesAbsentEmptyAndReportedZero() throws Exception {
        Codec<PQueryStatistics> codec = ProtobufProxy.create(PQueryStatistics.class);
        PQueryStatistics statistics = new PQueryStatistics();
        Assertions.assertNull(codec.decode(codec.encode(statistics)).aiStatistics);
        statistics.aiStatistics = new AIExecutionStatisticsPB();
        AIExecutionStatisticsPB empty = codec.decode(codec.encode(statistics)).aiStatistics;
        Assertions.assertNotNull(empty);
        Assertions.assertNull(empty.taskCount);
        Assertions.assertNull(empty.promptTokens);
        statistics.aiStatistics.promptTokens = 0L;
        statistics.aiStatistics.promptUsageCount = 1L;
        AIExecutionStatisticsPB zero = codec.decode(codec.encode(statistics)).aiStatistics;
        Assertions.assertEquals(0L, zero.promptTokens);
        Assertions.assertEquals(1L, zero.promptUsageCount);
        Assertions.assertNull(zero.completionTokens);
        Assertions.assertNull(zero.totalUsageCount);
    }

    @Test
    public void testLegacyReaderIgnoresAIWithoutChangingExistingOrdinals() throws Exception {
        PQueryStatistics statistics = new PQueryStatistics();
        statistics.scanRows = 7L;
        statistics.readLocalCnt = 12L;
        statistics.readRemoteCnt = 13L;
        statistics.aiStatistics = new AIExecutionStatisticsPB();
        statistics.aiStatistics.taskCount = 1L;
        Codec<PQueryStatistics> currentCodec = ProtobufProxy.create(PQueryStatistics.class);
        Codec<LegacyQueryStatistics> legacyCodec = ProtobufProxy.create(LegacyQueryStatistics.class);
        LegacyQueryStatistics legacy = legacyCodec.decode(currentCodec.encode(statistics));
        Assertions.assertEquals(7L, legacy.scanRows);
        Assertions.assertEquals(12L, legacy.readLocalCnt);
        Assertions.assertEquals(13L, legacy.readRemoteCnt);
        PQueryStatistics forwarded = currentCodec.decode(legacyCodec.encode(legacy));
        Assertions.assertNull(forwarded.aiStatistics);
        Assertions.assertEquals(12L, forwarded.readLocalCnt);
        Assertions.assertEquals(13L, forwarded.readRemoteCnt);
    }

    @Test
    public void testThriftPreservesNestedPresence() throws Exception {
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        TAuditStatistics decoded = new TAuditStatistics();
        deserializer.deserialize(decoded, serializer.serialize(new TAuditStatistics()));
        Assertions.assertFalse(decoded.isSetAi_statistics());

        TAuditStatistics statistics = new TAuditStatistics().setAi_statistics(new TAIExecutionStatistics());
        decoded = new TAuditStatistics();
        deserializer.deserialize(decoded, serializer.serialize(statistics));
        Assertions.assertTrue(decoded.isSetAi_statistics());
        Assertions.assertFalse(decoded.getAi_statistics().isSetPrompt_tokens());

        statistics.getAi_statistics().setPrompt_tokens(0).setPrompt_usage_count(1);
        decoded = new TAuditStatistics();
        deserializer.deserialize(decoded, serializer.serialize(statistics));
        Assertions.assertEquals(statistics, decoded);
        Assertions.assertTrue(decoded.getAi_statistics().isSetPrompt_tokens());
        Assertions.assertFalse(decoded.getAi_statistics().isSetCompletion_tokens());
    }

    // The pre-AI wire schema: an old reader skips field 14 and does not retain it when forwarding.
    public static class LegacyQueryStatistics {
        @Protobuf(fieldType = FieldType.INT64, order = 1, required = false)
        public Long scanRows;
        @Protobuf(fieldType = FieldType.INT64, order = 12, required = false)
        public Long readLocalCnt;
        @Protobuf(fieldType = FieldType.INT64, order = 13, required = false)
        public Long readRemoteCnt;
    }
}
