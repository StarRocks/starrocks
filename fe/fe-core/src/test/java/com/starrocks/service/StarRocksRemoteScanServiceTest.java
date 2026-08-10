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

package com.starrocks.service;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.Table;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.starrocks.StarRocksRemoteScanWire;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TAccessPathType;
import com.starrocks.thrift.TStarRocksRemoteScanRequiredOutput;
import com.starrocks.thrift.TStarRocksRemoteScanWireShape;
import com.starrocks.thrift.TStarRocksScanTransport;
import com.starrocks.type.IntegerType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.TypeSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the pure parts of the control-plane server: SQL synthesis, the pushdown deny
 * list, request-shape validation and the peer-forwarding budget. None of these need a cluster.
 *
 * <p>The synthesis tests matter because every name {@code buildProjection} emits comes from a
 * request the calling cluster fully controls, and the deny list ({@code findUnsafeReason}) only
 * guards the pushdown WHERE — the projection has no second line of defence, so it must both quote
 * safely and refuse to emit anything that is not in the local schema.
 */
public class StarRocksRemoteScanServiceTest {

    private static final SessionVariable SESSION_VARIABLE = new SessionVariable();

    private static String scanSql(String projection) {
        return "SELECT " + projection + " FROM `db1`.`tbl1`";
    }

    private static TStarRocksRemoteScanRequiredOutput prunedStructOutput(String subfield, Type expectedWireType) {
        ColumnAccessPath root = new ColumnAccessPath(TAccessPathType.ROOT, "s", InvalidType.INVALID);
        root.addChildPath(new ColumnAccessPath(TAccessPathType.FIELD, subfield, IntegerType.INT));

        TStarRocksRemoteScanRequiredOutput output = new TStarRocksRemoteScanRequiredOutput();
        output.setLocal_slot_id(0);
        output.setRoot_column("s");
        output.setWire_shape(TStarRocksRemoteScanWireShape.PRUNED_ROOT_STRUCT);
        output.setExpected_wire_type(TypeSerializer.toThrift(expectedWireType));
        output.setAccess_path(root.toThrift());
        return output;
    }

    private static StructType structOf(String... fieldNames) {
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : fieldNames) {
            fields.add(new StructField(fieldName, IntegerType.INT));
        }
        return new StructType(fields, true);
    }

    private static Map<String, Type> columnTypes(String column, Type type) {
        Map<String, Type> columnTypes = new HashMap<>();
        columnTypes.put(column, type);
        return columnTypes;
    }

    // ---- projection synthesis ----------------------------------------------

    /** Sanity: an ordinary subfield name round-trips into one parseable projection. */
    @Test
    public void testOrdinaryStructProjectionParses() throws StarRocksException {
        String projection = StarRocksRemoteScanService.buildProjection(
                prunedStructOutput("f1", structOf("f1")), 0, columnTypes("s", structOf("f1", "f2")));
        Assertions.assertEquals("named_struct('f1', `s`.`f1`) AS `__sr_out_0`", projection);
        Assertions.assertEquals(1, SqlParser.parse(scanSql(projection), SESSION_VARIABLE).size());
    }

    /** Only the access-path-selected fields survive, in the declared schema's field order. */
    @Test
    public void testPrunedStructKeepsOnlySelectedFieldsInSchemaOrder() throws StarRocksException {
        ColumnAccessPath root = new ColumnAccessPath(TAccessPathType.ROOT, "s", InvalidType.INVALID);
        root.addChildPath(new ColumnAccessPath(TAccessPathType.FIELD, "c", IntegerType.INT));
        root.addChildPath(new ColumnAccessPath(TAccessPathType.FIELD, "a", IntegerType.INT));
        TStarRocksRemoteScanRequiredOutput output = new TStarRocksRemoteScanRequiredOutput();
        output.setLocal_slot_id(0);
        output.setRoot_column("s");
        output.setWire_shape(TStarRocksRemoteScanWireShape.PRUNED_ROOT_STRUCT);
        output.setExpected_wire_type(TypeSerializer.toThrift(structOf("a", "c")));
        output.setAccess_path(root.toThrift());

        String projection = StarRocksRemoteScanService.buildProjection(
                output, 0, columnTypes("s", structOf("a", "b", "c")));
        // Declared order (a before c) regardless of the order the access path listed them, and "b"
        // is pruned away.
        Assertions.assertEquals("named_struct('a', `s`.`a`, 'c', `s`.`c`) AS `__sr_out_0`", projection);
        Assertions.assertEquals(1, SqlParser.parse(scanSql(projection), SESSION_VARIABLE).size());
    }

    /**
     * A trailing backslash in a subfield name used to escape its own closing quote and make the
     * whole synthesized statement unparseable — failing the entire prepare_scan, reachable with no
     * malice by a table that really has such a field. quoteStringLiteral now escapes backslashes.
     */
    @Test
    public void testTrailingBackslashSubfieldStillParses() throws StarRocksException {
        String projection = StarRocksRemoteScanService.buildProjection(
                prunedStructOutput("f1\\", structOf("f1\\")), 0, columnTypes("s", structOf("f1\\")));
        String sql = scanSql(projection);
        Assertions.assertDoesNotThrow(() -> SqlParser.parse(sql, SESSION_VARIABLE), "sql=" + sql);
    }

    @Test
    public void testQuoteStringLiteralEscapesBackslashBeforeQuote() {
        Assertions.assertEquals("'trailing\\\\'", StarRocksRemoteScanService.quoteStringLiteral("trailing\\"));
        Assertions.assertDoesNotThrow(() -> SqlParser.parse(
                "SELECT " + StarRocksRemoteScanService.quoteStringLiteral("trailing\\"), SESSION_VARIABLE));
        // Quotes stay doubled, and a backslash-quote pair escapes the backslash rather than the
        // quote (order of the two replacements matters).
        Assertions.assertEquals("'a''b'", StarRocksRemoteScanService.quoteStringLiteral("a'b"));
        Assertions.assertEquals("'a\\\\''b'", StarRocksRemoteScanService.quoteStringLiteral("a\\'b"));
        Assertions.assertDoesNotThrow(() -> SqlParser.parse(
                "SELECT " + StarRocksRemoteScanService.quoteStringLiteral("a\\'b"), SESSION_VARIABLE));
    }

    /**
     * Names are taken from the local schema, never from the request: a caller-supplied field name
     * that no declared field matches is simply not selected, so it cannot reach the SQL text.
     */
    @Test
    public void testRequestSuppliedFieldNamesNeverReachTheSql() {
        String hostile = "x', (SELECT 1) AS `y";
        Assertions.assertThrows(StarRocksException.class, () ->
                        StarRocksRemoteScanService.buildProjection(
                                prunedStructOutput(hostile, structOf(hostile)), 0,
                                columnTypes("s", structOf("a"))),
                "no declared field matches, so the pruned struct is empty and rejected");
    }

    @Test
    public void testUnknownOrNonStructRootColumnIsRejected() {
        // Unknown column.
        Assertions.assertThrows(StarRocksException.class, () ->
                StarRocksRemoteScanService.buildProjection(
                        prunedStructOutput("f1", structOf("f1")), 0, columnTypes("other", structOf("f1"))));
        // Declared as a scalar: PRUNED_ROOT_STRUCT makes no sense for it, and the old code would
        // have fallen back to quoting request-supplied names here.
        Assertions.assertThrows(StarRocksException.class, () ->
                StarRocksRemoteScanService.buildProjection(
                        prunedStructOutput("f1", IntegerType.INT), 0, columnTypes("s", IntegerType.INT)));
    }

    @Test
    public void testNestedStructPrunesRecursively() throws StarRocksException {
        StructType inner = new StructType(Arrays.asList(
                new StructField("x", IntegerType.INT), new StructField("y", IntegerType.INT)), true);
        StructType declared = new StructType(Arrays.asList(
                new StructField("a", IntegerType.INT), new StructField("b", inner)), true);

        ColumnAccessPath root = new ColumnAccessPath(TAccessPathType.ROOT, "s", InvalidType.INVALID);
        ColumnAccessPath b = new ColumnAccessPath(TAccessPathType.FIELD, "b", inner);
        b.addChildPath(new ColumnAccessPath(TAccessPathType.FIELD, "x", IntegerType.INT));
        root.addChildPath(b);
        TStarRocksRemoteScanRequiredOutput output = new TStarRocksRemoteScanRequiredOutput();
        output.setLocal_slot_id(0);
        output.setRoot_column("s");
        output.setWire_shape(TStarRocksRemoteScanWireShape.PRUNED_ROOT_STRUCT);
        output.setExpected_wire_type(TypeSerializer.toThrift(declared));
        output.setAccess_path(root.toThrift());

        String projection = StarRocksRemoteScanService.buildProjection(output, 0, columnTypes("s", declared));
        Assertions.assertEquals(
                "named_struct('b', named_struct('x', `s`.`b`.`x`)) AS `__sr_out_0`", projection);
        Assertions.assertEquals(1, SqlParser.parse(scanSql(projection), SESSION_VARIABLE).size());
    }

    @Test
    public void testFullRootAndRowMarkerProjections() throws StarRocksException {
        TStarRocksRemoteScanRequiredOutput fullRoot = new TStarRocksRemoteScanRequiredOutput();
        fullRoot.setLocal_slot_id(0);
        fullRoot.setRoot_column("c1");
        fullRoot.setWire_shape(TStarRocksRemoteScanWireShape.FULL_ROOT);
        fullRoot.setExpected_wire_type(TypeSerializer.toThrift(IntegerType.INT));
        Assertions.assertEquals("`c1` AS `__sr_out_0`",
                StarRocksRemoteScanService.buildProjection(fullRoot, 0, columnTypes("c1", IntegerType.INT)));

        TStarRocksRemoteScanRequiredOutput marker = new TStarRocksRemoteScanRequiredOutput();
        marker.setLocal_slot_id(1);
        marker.setWire_shape(TStarRocksRemoteScanWireShape.ROW_MARKER);
        marker.setExpected_wire_type(TypeSerializer.toThrift(IntegerType.BIGINT));
        Assertions.assertEquals("CAST(1 AS BIGINT) AS `__sr_out_1`",
                StarRocksRemoteScanService.buildProjection(marker, 1, Collections.emptyMap()));

        // ROW_MARKER must be a BIGINT: the local BE decodes that column positionally.
        TStarRocksRemoteScanRequiredOutput badMarker = new TStarRocksRemoteScanRequiredOutput();
        badMarker.setLocal_slot_id(2);
        badMarker.setWire_shape(TStarRocksRemoteScanWireShape.ROW_MARKER);
        badMarker.setExpected_wire_type(TypeSerializer.toThrift(IntegerType.INT));
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.buildProjection(badMarker, 2, Collections.emptyMap()));
    }

    /** An identifier carrying a backtick is doubled, so it cannot break out of the quoting. */
    @Test
    public void testBackquotedIdentifierIsEscaped() throws StarRocksException {
        TStarRocksRemoteScanRequiredOutput fullRoot = new TStarRocksRemoteScanRequiredOutput();
        fullRoot.setLocal_slot_id(0);
        fullRoot.setRoot_column("we`ird");
        fullRoot.setWire_shape(TStarRocksRemoteScanWireShape.FULL_ROOT);
        fullRoot.setExpected_wire_type(TypeSerializer.toThrift(IntegerType.INT));
        String projection = StarRocksRemoteScanService.buildProjection(
                fullRoot, 0, columnTypes("we`ird", IntegerType.INT));
        Assertions.assertEquals("`we``ird` AS `__sr_out_0`", projection);
        Assertions.assertEquals(1, SqlParser.parse(scanSql(projection), SESSION_VARIABLE).size());
    }

    // ---- request-shape validation ------------------------------------------

    @Test
    public void testValidateRequiredOutputRejectsIncompleteRequests() {
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.validateRequiredOutput(null, 0));

        TStarRocksRemoteScanRequiredOutput noSlot = new TStarRocksRemoteScanRequiredOutput();
        noSlot.setRoot_column("c1");
        noSlot.setExpected_wire_type(TypeSerializer.toThrift(IntegerType.INT));
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.validateRequiredOutput(noSlot, 0));

        TStarRocksRemoteScanRequiredOutput noRoot = new TStarRocksRemoteScanRequiredOutput();
        noRoot.setLocal_slot_id(0);
        noRoot.setExpected_wire_type(TypeSerializer.toThrift(IntegerType.INT));
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.validateRequiredOutput(noRoot, 0));

        TStarRocksRemoteScanRequiredOutput noType = new TStarRocksRemoteScanRequiredOutput();
        noType.setLocal_slot_id(0);
        noType.setRoot_column("c1");
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.validateRequiredOutput(noType, 0));

        TStarRocksRemoteScanRequiredOutput ok = new TStarRocksRemoteScanRequiredOutput();
        ok.setLocal_slot_id(0);
        ok.setRoot_column("c1");
        ok.setExpected_wire_type(TypeSerializer.toThrift(IntegerType.INT));
        Assertions.assertDoesNotThrow(() -> StarRocksRemoteScanService.validateRequiredOutput(ok, 0));
    }

    @Test
    public void testPrunedStructRequiresRootPathWithChildrenMatchingRootColumn() {
        // Root access path without children.
        ColumnAccessPath childless = new ColumnAccessPath(TAccessPathType.ROOT, "s", InvalidType.INVALID);
        TStarRocksRemoteScanRequiredOutput noChildren = new TStarRocksRemoteScanRequiredOutput();
        noChildren.setLocal_slot_id(0);
        noChildren.setRoot_column("s");
        noChildren.setWire_shape(TStarRocksRemoteScanWireShape.PRUNED_ROOT_STRUCT);
        noChildren.setExpected_wire_type(TypeSerializer.toThrift(structOf("f1")));
        noChildren.setAccess_path(childless.toThrift());
        Assertions.assertThrows(StarRocksException.class, () -> StarRocksRemoteScanService.buildProjection(
                noChildren, 0, columnTypes("s", structOf("f1"))));

        // root_column disagreeing with the access path root.
        TStarRocksRemoteScanRequiredOutput mismatched = prunedStructOutput("f1", structOf("f1"));
        mismatched.setRoot_column("other");
        Assertions.assertThrows(StarRocksException.class, () -> StarRocksRemoteScanService.buildProjection(
                mismatched, 0, columnTypes("other", structOf("f1"))));

        // PRUNED_ROOT_STRUCT without an access path at all.
        TStarRocksRemoteScanRequiredOutput noPath = new TStarRocksRemoteScanRequiredOutput();
        noPath.setLocal_slot_id(0);
        noPath.setRoot_column("s");
        noPath.setWire_shape(TStarRocksRemoteScanWireShape.PRUNED_ROOT_STRUCT);
        noPath.setExpected_wire_type(TypeSerializer.toThrift(structOf("f1")));
        Assertions.assertThrows(StarRocksException.class, () -> StarRocksRemoteScanService.buildProjection(
                noPath, 0, columnTypes("s", structOf("f1"))));
    }

    // ---- statement-shape validation ----------------------------------------

    private static StarRocksRemoteScanWire.PrepareScanRequest scanRequest(String pushdownPredicateSql) {
        StarRocksRemoteScanWire.PrepareScanRequest request = new StarRocksRemoteScanWire.PrepareScanRequest();
        request.db = "db1";
        request.table = "tbl1";
        request.pushdownPredicateSql = pushdownPredicateSql;
        return request;
    }

    @Test
    public void testParseAndValidateAcceptsSingleTableSelect() throws StarRocksException {
        StatementBase statement = StarRocksRemoteScanService.parseAndValidateRemoteScanStatement(
                "SELECT `c1` FROM `db1`.`tbl1`", SESSION_VARIABLE, scanRequest(null));
        Assertions.assertNotNull(statement);
    }

    @Test
    public void testParseAndValidateRejectsUnsupportedShapes() {
        // More than one statement.
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.parseAndValidateRemoteScanStatement(
                        "SELECT 1 FROM `db1`.`tbl1`; SELECT 2 FROM `db1`.`tbl1`",
                        SESSION_VARIABLE, scanRequest(null)));
        // Not a SELECT.
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.parseAndValidateRemoteScanStatement(
                        "SHOW DATABASES", SESSION_VARIABLE, scanRequest(null)));
        // Join / multiple relations.
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.parseAndValidateRemoteScanStatement(
                        "SELECT `c1` FROM `db1`.`tbl1` JOIN `db1`.`tbl2`", SESSION_VARIABLE, scanRequest(null)));
        // Aggregation / ordering clauses.
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.parseAndValidateRemoteScanStatement(
                        "SELECT `c1` FROM `db1`.`tbl1` GROUP BY `c1`", SESSION_VARIABLE, scanRequest(null)));
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.parseAndValidateRemoteScanStatement(
                        "SELECT `c1` FROM `db1`.`tbl1` ORDER BY `c1`", SESSION_VARIABLE, scanRequest(null)));
        // A table other than the requested one.
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.parseAndValidateRemoteScanStatement(
                        "SELECT `c1` FROM `db1`.`other`", SESSION_VARIABLE, scanRequest(null)));
    }

    @Test
    public void testParseAndValidateRejectsUnsafePushdownPredicate() {
        // A non-deterministic function must never be evaluated on the remote side.
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.parseAndValidateRemoteScanStatement(
                        "SELECT `c1` FROM `db1`.`tbl1`", SESSION_VARIABLE, scanRequest("`c1` > rand()")));
        // Nor a session variable, nor a subquery.
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.parseAndValidateRemoteScanStatement(
                        "SELECT `c1` FROM `db1`.`tbl1`", SESSION_VARIABLE, scanRequest("`c1` > @@query_timeout")));
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.parseAndValidateRemoteScanStatement(
                        "SELECT `c1` FROM `db1`.`tbl1`", SESSION_VARIABLE,
                        scanRequest("`c1` IN (SELECT `c1` FROM `db1`.`tbl1`)")));
        // A plain deterministic predicate is accepted.
        Assertions.assertDoesNotThrow(
                () -> StarRocksRemoteScanService.parseAndValidateRemoteScanStatement(
                        "SELECT `c1` FROM `db1`.`tbl1`", SESSION_VARIABLE, scanRequest("`c1` > 10")));
    }

    // ---- pushdown deny list ------------------------------------------------

    private static Expr parseExpression(String sql) {
        return SqlParser.parseExpression(sql, SESSION_VARIABLE);
    }

    @Test
    public void testFindUnsafeReasonDenyListMatrix() {
        // Portable: constants, arithmetic, deterministic functions, nested expressions.
        Assertions.assertNull(StarRocksRemoteScanService.findUnsafeReason(parseExpression("1 + 2")));
        Assertions.assertNull(StarRocksRemoteScanService.findUnsafeReason(parseExpression("abs(c1) > 3")));
        Assertions.assertNull(StarRocksRemoteScanService.findUnsafeReason(
                parseExpression("c1 > 1 AND lower(c2) = 'x'")));

        // Not portable: local session state.
        Assertions.assertNotNull(StarRocksRemoteScanService.findUnsafeReason(parseExpression("c1 = @@query_timeout")));
        Assertions.assertNotNull(StarRocksRemoteScanService.findUnsafeReason(parseExpression("c1 = current_user()")));
        Assertions.assertNotNull(StarRocksRemoteScanService.findUnsafeReason(parseExpression("c1 = database()")));
        Assertions.assertNotNull(StarRocksRemoteScanService.findUnsafeReason(
                parseExpression("c1 = connection_id()")));

        // Not portable: non-deterministic functions, including nested in a subtree.
        Assertions.assertNotNull(StarRocksRemoteScanService.findUnsafeReason(parseExpression("c1 > rand()")));
        Assertions.assertNotNull(StarRocksRemoteScanService.findUnsafeReason(
                parseExpression("c1 > 1 AND c2 < abs(rand())")));

        // Not portable: subqueries reference another query.
        Assertions.assertNotNull(StarRocksRemoteScanService.findUnsafeReason(
                parseExpression("c1 IN (SELECT c1 FROM db1.tbl1)")));
    }

    // ---- transport parsing -------------------------------------------------

    /**
     * An absent transport must fall back to the catalog default (brpc_chunk), and an unrecognized
     * one must be rejected instead of silently becoming Arrow Flight — whose ports the cluster may
     * not even have configured.
     */
    @Test
    public void testParseTransportDefaultsToBrpcAndRejectsUnknown() throws StarRocksException {
        Assertions.assertEquals(TStarRocksScanTransport.STARROCKS_BRPC_CHUNK,
                StarRocksRemoteScanService.parseTransport(StarRocksRemoteScanWire.TRANSPORT_BRPC_CHUNK));
        Assertions.assertEquals(TStarRocksScanTransport.STARROCKS_BRPC_CHUNK,
                StarRocksRemoteScanService.parseTransport("BRPC_CHUNK"));
        Assertions.assertEquals(TStarRocksScanTransport.STARROCKS_ARROW_FLIGHT,
                StarRocksRemoteScanService.parseTransport(StarRocksRemoteScanWire.TRANSPORT_ARROW_FLIGHT));
        Assertions.assertEquals(TStarRocksScanTransport.STARROCKS_ARROW_FLIGHT,
                StarRocksRemoteScanService.parseTransport("Arrow_Flight"));
        // Absent -> catalog default.
        Assertions.assertEquals(TStarRocksScanTransport.STARROCKS_BRPC_CHUNK,
                StarRocksRemoteScanService.parseTransport(null));
        Assertions.assertEquals(TStarRocksScanTransport.STARROCKS_BRPC_CHUNK,
                StarRocksRemoteScanService.parseTransport(""));
        // Typo / unknown -> rejected.
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.parseTransport("brpc"));
        Assertions.assertThrows(StarRocksException.class,
                () -> StarRocksRemoteScanService.parseTransport("grpc_chunk"));
    }

    // ---- scan SQL assembly -------------------------------------------------

    private static TStarRocksRemoteScanRequiredOutput fullRootOutput(int slotId, String column) {
        TStarRocksRemoteScanRequiredOutput output = new TStarRocksRemoteScanRequiredOutput();
        output.setLocal_slot_id(slotId);
        output.setRoot_column(column);
        output.setWire_shape(TStarRocksRemoteScanWireShape.FULL_ROOT);
        output.setExpected_wire_type(TypeSerializer.toThrift(IntegerType.INT));
        return output;
    }

    @Test
    public void testBuildScanSqlQuotesTableAndAppendsSoftLimit() throws StarRocksException {
        StarRocksRemoteScanWire.PrepareScanRequest request = scanRequest(null);
        Map<String, Type> types = new HashMap<>();
        types.put("c1", IntegerType.INT);
        types.put("c2", IntegerType.INT);
        List<TStarRocksRemoteScanRequiredOutput> outputs =
                Arrays.asList(fullRootOutput(0, "c1"), fullRootOutput(1, "c2"));

        Assertions.assertEquals(
                "SELECT `c1` AS `__sr_out_0`, `c2` AS `__sr_out_1` FROM `db1`.`tbl1`",
                StarRocksRemoteScanService.buildScanSql(request, outputs, types));

        request.softLimit = 10;
        Assertions.assertEquals(
                "SELECT `c1` AS `__sr_out_0`, `c2` AS `__sr_out_1` FROM `db1`.`tbl1` LIMIT 10",
                StarRocksRemoteScanService.buildScanSql(request, outputs, types));

        // A non-positive soft limit means "no limit" and must not reach the SQL.
        request.softLimit = -1;
        Assertions.assertFalse(StarRocksRemoteScanService.buildScanSql(request, outputs, types).contains("LIMIT"));
        request.softLimit = 0;
        Assertions.assertFalse(StarRocksRemoteScanService.buildScanSql(request, outputs, types).contains("LIMIT"));
    }

    /**
     * Zero required outputs would otherwise force a "SELECT *", whose schema-order columns do not
     * line up with what the local BE decodes positionally — reject it loudly.
     */
    @Test
    public void testBuildScanSqlRejectsEmptyRequiredOutputs() {
        Assertions.assertThrows(StarRocksException.class, () -> StarRocksRemoteScanService.buildScanSql(
                scanRequest(null), Collections.emptyList(), Collections.emptyMap()));
    }

    // ---- column access path merging ----------------------------------------

    private static ColumnAccessPath rootPath(String column, boolean fromPredicate, String... children) {
        ColumnAccessPath root = new ColumnAccessPath(TAccessPathType.ROOT, column, InvalidType.INVALID);
        root.setFromPredicate(fromPredicate);
        for (String child : children) {
            ColumnAccessPath childPath = new ColumnAccessPath(TAccessPathType.FIELD, child, IntegerType.INT);
            childPath.setFromPredicate(fromPredicate);
            root.addChildPath(childPath);
        }
        return root;
    }

    private static List<String> childNames(ColumnAccessPath path) {
        List<String> names = new ArrayList<>();
        for (ColumnAccessPath child : path.getChildren()) {
            names.add(child.getPath());
        }
        Collections.sort(names);
        return names;
    }

    /**
     * fromPredicate is merged with AND on purpose: a path is predicate-only only when every
     * contributor is, so one output contributor makes the merged path an output path. Children are
     * unioned so the pruned struct covers predicate and output subfields alike.
     */
    @Test
    public void testMergeColumnAccessPathsUnionsChildrenAndResolvesPredicateFlag() {
        // Remote-planned predicate-only path + client output path for the same column.
        List<ColumnAccessPath> merged = StarRocksRemoteScanService.mergeColumnAccessPaths(
                Collections.singletonList(rootPath("s", true, "pred")),
                Collections.singletonList(rootPath("s", false, "out")));
        Assertions.assertEquals(1, merged.size());
        Assertions.assertFalse(merged.get(0).isFromPredicate(), "an output contributor wins");
        Assertions.assertEquals(Arrays.asList("out", "pred"), childNames(merged.get(0)));

        // Predicate-only on both sides stays predicate-only.
        List<ColumnAccessPath> predicateOnly = StarRocksRemoteScanService.mergeColumnAccessPaths(
                Collections.singletonList(rootPath("s", true, "a")),
                Collections.singletonList(rootPath("s", true, "b")));
        Assertions.assertTrue(predicateOnly.get(0).isFromPredicate());
        Assertions.assertEquals(Arrays.asList("a", "b"), childNames(predicateOnly.get(0)));

        // Different columns coexist rather than merging.
        List<ColumnAccessPath> distinct = StarRocksRemoteScanService.mergeColumnAccessPaths(
                Collections.singletonList(rootPath("s1", false, "a")),
                Collections.singletonList(rootPath("s2", false, "b")));
        Assertions.assertEquals(2, distinct.size());

        // Column names match case-insensitively.
        List<ColumnAccessPath> caseInsensitive = StarRocksRemoteScanService.mergeColumnAccessPaths(
                Collections.singletonList(rootPath("S", false, "a")),
                Collections.singletonList(rootPath("s", false, "b")));
        Assertions.assertEquals(1, caseInsensitive.size());
        Assertions.assertEquals(Arrays.asList("a", "b"), childNames(caseInsensitive.get(0)));

        // Nulls and empties are tolerated.
        Assertions.assertTrue(StarRocksRemoteScanService.mergeColumnAccessPaths(null, null).isEmpty());
        Assertions.assertEquals(1, StarRocksRemoteScanService.mergeColumnAccessPaths(
                null, Collections.singletonList(rootPath("s", false, "a"))).size());
    }

    /** The merge must not mutate the inputs: planned paths belong to the remote's own plan. */
    @Test
    public void testMergeColumnAccessPathsDoesNotMutateInputs() {
        ColumnAccessPath planned = rootPath("s", true, "pred");
        StarRocksRemoteScanService.mergeColumnAccessPaths(
                Collections.singletonList(planned), Collections.singletonList(rootPath("s", false, "out")));
        Assertions.assertTrue(planned.isFromPredicate(), "input flag untouched");
        Assertions.assertEquals(Collections.singletonList("pred"), childNames(planned));
    }

    @Test
    public void testOutputStructAccessPathsKeepsOnlyOutputRootsWithChildren() {
        ColumnAccessPath output = rootPath("s", false, "a");
        ColumnAccessPath predicateOnly = rootPath("p", true, "a");
        ColumnAccessPath childless = rootPath("c", false);
        ColumnAccessPath extended = rootPath("e", false, "a");
        extended.setExtended(true);

        Map<String, ColumnAccessPath> outputs = StarRocksRemoteScanService.outputStructAccessPaths(
                Arrays.asList(output, predicateOnly, childless, extended));

        Assertions.assertEquals(Collections.singleton("s"), outputs.keySet());
        Assertions.assertTrue(StarRocksRemoteScanService.outputStructAccessPaths(null).isEmpty());
        Assertions.assertTrue(
                StarRocksRemoteScanService.outputStructAccessPaths(Collections.emptyList()).isEmpty());
    }

    @Test
    public void testPruneStructTypeKeepsSelectedFieldsAndDegradesSafely() {
        StructType inner = new StructType(Arrays.asList(
                new StructField("x", IntegerType.INT), new StructField("y", IntegerType.INT)), true);
        StructType declared = new StructType(Arrays.asList(
                new StructField("a", IntegerType.INT), new StructField("b", inner)), true);

        ColumnAccessPath path = new ColumnAccessPath(TAccessPathType.ROOT, "s", InvalidType.INVALID);
        ColumnAccessPath b = new ColumnAccessPath(TAccessPathType.FIELD, "b", inner);
        b.addChildPath(new ColumnAccessPath(TAccessPathType.FIELD, "x", IntegerType.INT));
        path.addChildPath(b);

        Type pruned = StarRocksRemoteScanService.pruneStructType(declared, path);
        Assertions.assertTrue(pruned.isStructType());
        StructType prunedStruct = (StructType) pruned;
        Assertions.assertEquals(1, prunedStruct.getFields().size());
        Assertions.assertEquals("b", prunedStruct.getFields().get(0).getName());
        Assertions.assertEquals(1, ((StructType) prunedStruct.getFields().get(0).getType()).getFields().size());

        // Nothing selected: keep the declared type rather than producing an empty struct.
        Assertions.assertSame(declared, StarRocksRemoteScanService.pruneStructType(
                declared, new ColumnAccessPath(TAccessPathType.ROOT, "s", InvalidType.INVALID)));
        // A scalar type is returned untouched.
        Assertions.assertSame(IntegerType.INT,
                StarRocksRemoteScanService.pruneStructType(IntegerType.INT, path));
    }

    // ---- access path parsing -----------------------------------------------

    @Test
    public void testParseColumnAccessPathsFromRequiredOutputsPicksPrunedStructsOnly() throws StarRocksException {
        List<TStarRocksRemoteScanRequiredOutput> outputs = Arrays.asList(
                prunedStructOutput("f1", structOf("f1")), fullRootOutput(1, "c1"), null);
        List<ColumnAccessPath> paths =
                StarRocksRemoteScanService.parseColumnAccessPathsFromRequiredOutputs(outputs);
        Assertions.assertEquals(1, paths.size());
        Assertions.assertEquals("s", paths.get(0).getPath());

        Assertions.assertTrue(
                StarRocksRemoteScanService.parseColumnAccessPathsFromRequiredOutputs(null).isEmpty());
        Assertions.assertTrue(StarRocksRemoteScanService
                .parseColumnAccessPathsFromRequiredOutputs(Collections.emptyList()).isEmpty());
    }

    @Test
    public void testParseColumnAccessPathsFromWireDtos() throws StarRocksException {
        StarRocksRemoteScanWire.ColumnAccessPathDto dto =
                StarRocksRemoteScanWire.toDto(rootPath("s", false, "a"));
        List<ColumnAccessPath> paths =
                StarRocksRemoteScanService.parseColumnAccessPaths(Collections.singletonList(dto));
        Assertions.assertEquals(1, paths.size());
        Assertions.assertEquals("s", paths.get(0).getPath());
        Assertions.assertEquals(Collections.singletonList("a"), childNames(paths.get(0)));

        Assertions.assertTrue(StarRocksRemoteScanService.parseColumnAccessPaths(null).isEmpty());
        Assertions.assertTrue(
                StarRocksRemoteScanService.parseColumnAccessPaths(Collections.emptyList()).isEmpty());
    }

    // ---- session registration (the duplicate-prepare guard) -----------------

    private static StarRocksRemoteScanService.RemoteScanOwner owner(String user) {
        ConnectContext context = ConnectContext.buildInner();
        context.setQualifiedUser(user);
        return StarRocksRemoteScanService.RemoteScanOwner.fromContext(context);
    }

    private static StarRocksRemoteScanService.RemoteScanContext preparedContext(String sessionId, String db,
                                                                               String table, String token) {
        return preparedContext(sessionId, db, table, token, owner("alice"));
    }

    private static StarRocksRemoteScanService.RemoteScanContext preparedContext(
            String sessionId, String db, String table, String token,
            StarRocksRemoteScanService.RemoteScanOwner owner) {
        // A null coordinator models the EMPTYSET plan: nothing to deploy, so registration and
        // supersede bookkeeping can be exercised without a cluster.
        return new StarRocksRemoteScanService.RemoteScanContext(sessionId, db, table, token, null,
                token, null, Collections.emptyList(), Collections.emptyList(),
                System.currentTimeMillis() + 60_000, owner);
    }

    @Test
    public void testRemoteScanContextSameTableAndSupersedeClaim() {
        StarRocksRemoteScanService.RemoteScanContext first = preparedContext("s1", "db1", "tbl1", "t1");
        StarRocksRemoteScanService.RemoteScanContext sameTable = preparedContext("s1", "DB1", "TBL1", "t2");
        StarRocksRemoteScanService.RemoteScanContext otherTable = preparedContext("s1", "db1", "tbl2", "t3");

        Assertions.assertTrue(first.isSameTable(sameTable), "db/table compare case-insensitively");
        Assertions.assertFalse(first.isSameTable(otherTable));

        // The claim is one-shot: a second attempt fails so two concurrent prepares cannot both
        // drop the same context.
        Assertions.assertTrue(first.trySupersede());
        Assertions.assertFalse(first.trySupersede());
    }

    /**
     * A retried prepare_scan reuses the session id (it is the execution id). Since
     * startRemoteScanSession starts every context in the session, the superseded one would scan
     * this cluster a second time with nobody consuming it — so registration must drop it.
     */
    @Test
    public void testRegisterRemoteScanSupersedesRetriedPrepareForSameTable() throws StarRocksException {
        StarRocksRemoteScanService service = new StarRocksRemoteScanService();
        StarRocksRemoteScanService.RemoteScanContext first = preparedContext("session-1", "db1", "tbl1", "t1");
        StarRocksRemoteScanService.RemoteScanContext retried = preparedContext("session-1", "db1", "tbl1", "t2");

        service.registerRemoteScan(first);
        Assertions.assertEquals(1, service.preparedScansForTest("session-1").size());

        service.registerRemoteScan(retried);
        List<StarRocksRemoteScanService.RemoteScanContext> scans = service.preparedScansForTest("session-1");
        Assertions.assertEquals(1, scans.size(), "the earlier prepare is dropped, not accumulated");
        Assertions.assertSame(retried, scans.get(0));
    }

    /** One query may legitimately hold several scans — different tables must all be kept. */
    @Test
    public void testRegisterRemoteScanKeepsDifferentTablesInOneSession() throws StarRocksException {
        StarRocksRemoteScanService service = new StarRocksRemoteScanService();
        service.registerRemoteScan(preparedContext("session-2", "db1", "tbl1", "t1"));
        service.registerRemoteScan(preparedContext("session-2", "db1", "tbl2", "t2"));
        Assertions.assertEquals(2, service.preparedScansForTest("session-2").size());
    }

    /** An already-started context is never dropped: the client may be reading its streams. */
    @Test
    public void testRegisterRemoteScanKeepsAlreadyClaimedContext() throws StarRocksException {
        StarRocksRemoteScanService service = new StarRocksRemoteScanService();
        StarRocksRemoteScanService.RemoteScanContext first = preparedContext("session-3", "db1", "tbl1", "t1");
        service.registerRemoteScan(first);
        // Simulate "already started / already claimed" by taking the claim first.
        Assertions.assertTrue(first.trySupersede());

        service.registerRemoteScan(preparedContext("session-3", "db1", "tbl1", "t2"));
        Assertions.assertEquals(2, service.preparedScansForTest("session-3").size(),
                "a context that can no longer be claimed stays registered");
    }

    /** A session id is caller-supplied, so a different user must not be able to join one. */
    @Test
    public void testRegisterRemoteScanRejectsForeignOwner() throws StarRocksException {
        StarRocksRemoteScanService service = new StarRocksRemoteScanService();
        service.registerRemoteScan(preparedContext("session-4", "db1", "tbl1", "t1", owner("alice")));
        Assertions.assertThrows(StarRocksException.class, () -> service.registerRemoteScan(
                preparedContext("session-4", "db1", "tbl1", "t2", owner("bob"))));
        Assertions.assertEquals(1, service.preparedScansForTest("session-4").size());
    }

    @Test
    public void testPreparedScansForUnknownSessionIsEmpty() {
        Assertions.assertTrue(new StarRocksRemoteScanService().preparedScansForTest("nope").isEmpty());
    }

    // ---- peer forwarding budget --------------------------------------------

    /**
     * The slice is the REMAINING budget divided by the peers still to probe, which is what keeps
     * one hung peer from consuming everything and starving the FE that owns the session.
     */
    @Test
    public void testForwardSliceDividesRemainingBudgetAcrossRemainingPeers() {
        long now = System.currentTimeMillis();
        long deadlineMs = now + 8000;

        int firstOfThree = StarRocksRemoteScanService.forwardSliceMs(deadlineMs, 3);
        Assertions.assertTrue(firstOfThree > 2000 && firstOfThree <= 2667,
                "roughly a third of the budget, actual " + firstOfThree);

        // The last peer may use everything that is left.
        int lastOfOne = StarRocksRemoteScanService.forwardSliceMs(deadlineMs, 1);
        Assertions.assertTrue(lastOfOne > firstOfThree * 2, "actual " + lastOfOne);

        // An exhausted (or already passed) deadline never yields a negative or usable slice.
        Assertions.assertEquals(0, StarRocksRemoteScanService.forwardSliceMs(now - 1000, 2));
        Assertions.assertEquals(0, StarRocksRemoteScanService.forwardSliceMs(deadlineMs, 0));
    }

    @Test
    public void testStringTypedStructFieldProjectionParses() throws StarRocksException {
        StructType declared = new StructType(Collections.singletonList(
                new StructField("name", TypeFactory.createDefaultCatalogString())), true);
        String projection = StarRocksRemoteScanService.buildProjection(
                prunedStructOutput("name", declared), 0, columnTypes("s", declared));
        Assertions.assertEquals("named_struct('name', `s`.`name`) AS `__sr_out_0`", projection);
        Assertions.assertEquals(1, SqlParser.parse(scanSql(projection), SESSION_VARIABLE).size());
    }

    // The get-table payload must carry the table id: the consumer builds
    // StarRocksExternalTable.getUUID() out of it, and without it every connector-statistics
    // lookup for this table throws instead of resolving.
    @Test
    public void testWireTableReportsTableId() {
        Table table = new Table(10086L, "tbl1", Table.TableType.STARROCKS,
                Collections.singletonList(new Column("k", IntegerType.INT, true)));

        StarRocksRemoteScanWire.Table wireTable =
                StarRocksRemoteScanService.toWireTableLocked("db1", "tbl1", table);

        Assertions.assertEquals(10086L, wireTable.tableId);
        Assertions.assertEquals("db1", wireTable.db);
        Assertions.assertEquals("tbl1", wireTable.table);
        Assertions.assertEquals(1, wireTable.columns.size());
        Assertions.assertEquals("k", wireTable.columns.get(0).name);
    }
}
