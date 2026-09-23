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

package com.starrocks.connector.paimon;

import com.starrocks.connector.index.ConnectorIndexType;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class PaimonGlobalIndexRequestTest {
    @Test
    public void testJsonAndTransportRoundTrip() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.GE, column, ConstantOperator.createInt(10));
        PaimonGlobalIndexRequest request = PaimonGlobalIndexRequest.create(
                42L, new IndexCondition(predicate, Map.of("k", ConnectorIndexType.RANGE)));

        PaimonGlobalIndexRequest fromJson = PaimonGlobalIndexRequest.parse(request.toJson());
        PaimonGlobalIndexRequest fromTransport =
                PaimonGlobalIndexRequest.parseTransport(request.toTransportString());

        Assertions.assertEquals(42L, fromJson.getSnapshotId());
        Assertions.assertEquals(Map.of("k", ConnectorIndexType.RANGE), fromJson.getRequiredIndexes());
        Assertions.assertEquals(42L, fromTransport.getSnapshotId());
        Assertions.assertEquals(Map.of("k", ConnectorIndexType.RANGE), fromTransport.getRequiredIndexes());
        Assertions.assertEquals(request.toJson(), fromJson.toJson());
        Assertions.assertEquals(request.toJson(), fromTransport.toJson());
        Assertions.assertTrue(fromTransport.toJson().contains("\"v\":10"));
        Assertions.assertFalse(fromTransport.toJson().contains("\"v\":10.0"));
        Assertions.assertFalse(request.toTransportString().contains("\"snapshotId\""));
    }

    @Test
    public void testRejectsInvalidEnvelope() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parse("{\"version\":2,\"snapshotId\":1}"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parseTransport("not-base64"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexRequest.parse(
                        "{\"version\":1,\"snapshotId\":1,\"predicate\":{},"
                                + "\"indexes\":{\"k\":\"unknown\"}}"));
    }
}
