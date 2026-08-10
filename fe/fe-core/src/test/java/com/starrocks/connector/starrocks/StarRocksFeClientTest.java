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

package com.starrocks.connector.starrocks;

import com.google.gson.Gson;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.thrift.TAccessPathType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStarRocksRemoteScanOutput;
import com.starrocks.thrift.TStarRocksRemoteScanRequiredOutput;
import com.starrocks.thrift.TStarRocksRemoteScanWireShape;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class StarRocksFeClientTest {
    @Test
    public void testParseComplexTypes() {
        Type arrayType = StarRocksFeClient.parseType("array<int>");
        Assertions.assertTrue(arrayType instanceof ArrayType);

        Type mapType = StarRocksFeClient.parseType("map<varchar(20), array<bigint>>");
        Assertions.assertTrue(mapType instanceof MapType);

        Type structType = StarRocksFeClient.parseType("struct<a int, b array<varchar(10)>>");
        Assertions.assertTrue(structType instanceof StructType);
    }

    @Test
    public void testParseFeAddressesSingleEndpointDefaultsToHttp() {
        List<String> addresses = StarRocksFeClient.parseFeAddresses("host1:8030");
        Assertions.assertEquals(List.of("http://host1:8030"), addresses);
    }

    @Test
    public void testParseFeAddressesMultipleEndpoints() {
        List<String> addresses =
                StarRocksFeClient.parseFeAddresses("host1:8030, host2:8031 ,host3:8032");
        Assertions.assertEquals(List.of("http://host1:8030", "http://host2:8031", "http://host3:8032"), addresses);
    }

    @Test
    public void testParseFeAddressesKeepsExplicitScheme() {
        Assertions.assertEquals(List.of("http://host1:8030"),
                StarRocksFeClient.parseFeAddresses("http://host1:8030"));
        Assertions.assertEquals(List.of("https://host1:8030"),
                StarRocksFeClient.parseFeAddresses("https://host1:8030"));
        // Trailing slash and mixed schemes in one list.
        Assertions.assertEquals(List.of("https://host1:8030", "http://host2:8031", "http://host3:8032"),
                StarRocksFeClient.parseFeAddresses("https://host1:8030/, http://host2:8031, host3:8032"));
    }

    @Test
    public void testParseFeAddressesRejectsUnknownScheme() {
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> StarRocksFeClient.parseFeAddresses("ftp://host1:8030"));
    }

    @Test
    public void testParseFeAddressNormalizesSchemeAndTrailingSlash() {
        Assertions.assertEquals("http://host1:8030", StarRocksFeClient.parseFeAddress("host1:8030"));
        Assertions.assertEquals("http://host1:8030", StarRocksFeClient.parseFeAddress("http://host1:8030"));
        Assertions.assertEquals("https://host1:8030", StarRocksFeClient.parseFeAddress("https://host1:8030"));
        // Scheme matching is case-insensitive and normalized to lower case.
        Assertions.assertEquals("https://host1:8030", StarRocksFeClient.parseFeAddress("HTTPS://host1:8030"));
        Assertions.assertEquals("https://host1:8030", StarRocksFeClient.parseFeAddress("https://host1:8030/"));
    }

    @Test
    public void testParseFeAddressRejectsMalformedEndpoint() {
        // Unsupported scheme.
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> StarRocksFeClient.parseFeAddress("ftp://host1:8030"));
        // Missing port, non-numeric port, missing host, scheme without port.
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> StarRocksFeClient.parseFeAddress("host1"));
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> StarRocksFeClient.parseFeAddress("host1:port"));
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> StarRocksFeClient.parseFeAddress(":8030"));
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> StarRocksFeClient.parseFeAddress("http://host1"));
    }

    @Test
    public void testParseFeAddressesRejectsEmpty() {
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> StarRocksFeClient.parseFeAddresses(""));
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> StarRocksFeClient.parseFeAddresses(" , "));
    }

    @Test
    public void testParseFeAddressesRejectsMalformedEndpoint() {
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> StarRocksFeClient.parseFeAddresses("host1:8030,not_a_host"));
    }

    // Wire converter: ColumnAccessPath domain -> JSON DTO -> domain preserves the tree shape.
    @Test
    public void testColumnAccessPathWireRoundTrip() {
        ColumnAccessPath root = ColumnAccessPath.createLinearPath(
                java.util.Arrays.asList("s", "a", "b"), IntegerType.INT);
        root.setFromPredicate(true);

        StarRocksRemoteScanWire.ColumnAccessPathDto dto = StarRocksRemoteScanWire.toDto(root);
        Assertions.assertEquals("ROOT", dto.type);
        Assertions.assertEquals("s", dto.path);
        Assertions.assertTrue(dto.fromPredicate);
        Assertions.assertEquals(1, dto.children.size());
        Assertions.assertEquals("a", dto.children.get(0).path);
        Assertions.assertEquals("b", dto.children.get(0).children.get(0).path);

        ColumnAccessPath restored = StarRocksRemoteScanWire.toDomain(dto);
        Assertions.assertEquals(TAccessPathType.ROOT, restored.getType());
        Assertions.assertEquals("s", restored.getPath());
        Assertions.assertTrue(restored.isFromPredicate());
        Assertions.assertEquals("a", restored.getChildren().get(0).getPath());
        Assertions.assertEquals(TAccessPathType.FIELD, restored.getChildren().get(0).getType());
        Assertions.assertEquals("b", restored.getChildren().get(0).getChildren().get(0).getPath());
    }

    // Wire converter: required output thrift -> JSON DTO -> thrift preserves the plan binding.
    @Test
    public void testRequiredOutputWireRoundTrip() {
        TStarRocksRemoteScanRequiredOutput required = new TStarRocksRemoteScanRequiredOutput();
        required.setLocal_slot_id(5);
        required.setRoot_column("k1");
        required.setWire_shape(TStarRocksRemoteScanWireShape.FULL_ROOT);
        required.setExpected_wire_type(TypeSerializer.toThrift(IntegerType.BIGINT));

        StarRocksRemoteScanWire.RequiredOutput dto = StarRocksRemoteScanWire.toDto(required);
        Assertions.assertEquals(5, dto.localSlotId);
        Assertions.assertEquals("k1", dto.rootColumn);
        Assertions.assertEquals("FULL_ROOT", dto.wireShape);
        Assertions.assertEquals("bigint(20)", dto.expectedWireType);

        TStarRocksRemoteScanRequiredOutput restored = StarRocksRemoteScanWire.toThrift(dto);
        Assertions.assertEquals(5, restored.local_slot_id);
        Assertions.assertEquals("k1", restored.root_column);
        Assertions.assertEquals(TStarRocksRemoteScanWireShape.FULL_ROOT, restored.wire_shape);
        Assertions.assertTrue(restored.isSetExpected_wire_type());
    }

    // Wire converter: scan output thrift -> JSON DTO -> thrift preserves the output contract.
    @Test
    public void testScanOutputWireRoundTrip() {
        TStarRocksRemoteScanOutput output = new TStarRocksRemoteScanOutput();
        output.setOutput_index(2);
        output.setLocal_slot_id(7);
        output.setName("__sr_out_2");
        output.setActual_wire_type(TypeSerializer.toThrift(IntegerType.BIGINT));
        output.setNullable(true);
        output.setIs_const(false);
        output.setWire_shape(TStarRocksRemoteScanWireShape.ROW_MARKER);

        StarRocksRemoteScanWire.ScanOutput dto = StarRocksRemoteScanWire.toDto(output);
        Assertions.assertEquals(2, dto.outputIndex);
        Assertions.assertEquals(7, dto.localSlotId);
        Assertions.assertEquals("__sr_out_2", dto.name);
        Assertions.assertEquals("bigint(20)", dto.actualWireType);
        Assertions.assertTrue(dto.nullable);
        Assertions.assertFalse(dto.isConst);
        Assertions.assertEquals("ROW_MARKER", dto.wireShape);

        TStarRocksRemoteScanOutput restored = StarRocksRemoteScanWire.toThrift(dto);
        Assertions.assertEquals(2, restored.output_index);
        Assertions.assertEquals(7, restored.local_slot_id);
        Assertions.assertEquals("__sr_out_2", restored.name);
        Assertions.assertTrue(restored.nullable);
        Assertions.assertEquals(TStarRocksRemoteScanWireShape.ROW_MARKER, restored.wire_shape);
    }

    // host:port DTO round-trip used by prepare-scan streams.
    @Test
    public void testHostPortWireRoundTrip() {
        TNetworkAddress address = new TNetworkAddress("be-1", 8060);
        StarRocksRemoteScanWire.HostPort dto = StarRocksRemoteScanWire.toDto(address);
        Assertions.assertEquals("be-1", dto.host);
        Assertions.assertEquals(8060, dto.port);

        TNetworkAddress restored = StarRocksRemoteScanWire.toThrift(dto);
        Assertions.assertEquals("be-1", restored.hostname);
        Assertions.assertEquals(8060, restored.port);
    }

    // get-table payload carries the remote table id, the incarnation marker behind
    // StarRocksExternalTable.getUUID.
    @Test
    public void testGetTableResponseCarriesTableId() {
        Gson gson = new Gson();

        StarRocksRemoteScanWire.Table parsed = gson.fromJson(
                "{\"db\":\"db1\",\"table\":\"tbl1\",\"schema_version\":3,\"row_count\":10,\"table_id\":10086}",
                StarRocksRemoteScanWire.Table.class);
        Assertions.assertEquals(10086L, parsed.tableId);
        Assertions.assertEquals(3, parsed.schemaVersion);

        StarRocksRemoteScanWire.Table emitted = new StarRocksRemoteScanWire.Table();
        emitted.tableId = 10086L;
        Assertions.assertTrue(gson.toJson(emitted).contains("\"table_id\":10086"));
    }
}
