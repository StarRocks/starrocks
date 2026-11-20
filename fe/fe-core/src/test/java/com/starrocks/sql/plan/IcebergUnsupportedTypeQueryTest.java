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

package com.starrocks.sql.plan;

import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;

/**
 * Verifies that querying Iceberg columns with unsupported V3 types
 * (e.g., geometry with an unsupported CRS, timestamp_nano) fails at analysis time with a clear error,
 * while querying supported columns on the same table works fine.
 */
public class IcebergUnsupportedTypeQueryTest extends ConnectorPlanTestBase {
    @Test
    public void testGeographyAccessAndNestedOutput() throws Exception {
        String table = "iceberg0.unpartitioned_db.t_unknown_types";
        String sql = "SELECT ST_AsText(geography_col), ST_AsText(locations[1].shape), "
                + "locations[1].label FROM " + table + " WHERE geography_col IS NOT NULL";
        assertContains(planQuery(sql, TExplainLevel.NORMAL), "IcebergScanNode");

        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> getFragmentPlan("SELECT locations FROM " + table));
        assertContains(ex.getMessage(), "Nested GEOGRAPHY/GEOMETRY query results are not supported");
        ex = Assertions.assertThrows(SemanticException.class,
                () -> getFragmentPlan("CREATE VIEW nested_iceberg_geo AS " + sql));
        assertContains(ex.getMessage(), "Nested GEOGRAPHY/GEOMETRY types are not supported in saved SQL definitions");
    }

    @Test
    public void testNestedGeometryAccessAndPruning() throws Exception {
        String from = " FROM iceberg0.unpartitioned_db.t_unknown_types";
        String sql = "SELECT ST_AsText(geom_scalar), ST_AsText(geom_array[1]), ST_AsBinary(geom_map[1]), "
                + "ST_AsText(geom_struct.shape), ST_AsText(geom_locations[1].shape), id" + from;
        assertContains(planQuery(sql, TExplainLevel.NORMAL), "IcebergScanNode");

        assertContains(planQuery("SELECT ST_AsText(geom_struct.shape)" + from, TExplainLevel.VERBOSE),
                "ColumnAccessPath: [/geom_struct/shape]", "struct<`shape` GEOMETRY>");
        assertContains(planQuery("SELECT geom_struct.label" + from, TExplainLevel.VERBOSE),
                "ColumnAccessPath: [/geom_struct/label]", "struct<`label` varchar");
    }

    private String planQuery(String sql, TExplainLevel level) throws Exception {
        var statement = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
        return StatementPlanner.plan(statement, connectContext).getExplainString(level);
    }

    @Test
    public void testNestedGeometryRefusals() {
        String from = " FROM iceberg0.unpartitioned_db.t_unknown_types";
        for (String container : List.of("geom_array", "geom_map", "geom_struct", "geom_locations")) {
            SemanticException ex = Assertions.assertThrows(SemanticException.class,
                    () -> getFragmentPlan("SELECT " + container + from));
            assertContains(ex.getMessage(), "Nested GEOGRAPHY/GEOMETRY query results are not supported");
        }

        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> getFragmentPlan("SELECT geom_struct.shape" + from));
        assertContains(ex.getMessage(), "GEOMETRY query results are not supported");
        ex = Assertions.assertThrows(SemanticException.class,
                () -> getFragmentPlan("CREATE VIEW nested_iceberg_geometry AS SELECT ST_AsText(geom_array[1])" + from));
        assertContains(ex.getMessage(), "Nested GEOGRAPHY/GEOMETRY types are not supported in saved SQL definitions");
        ex = Assertions.assertThrows(SemanticException.class,
                () -> getFragmentPlan("SELECT ST_AsText(bad_geom_array[1])" + from));
        assertContains(ex.getMessage(), "Datatype of external table column [bad_geom_array] is not supported");
    }

    @Test
    public void testUnsupportedGeometryCastFails() {
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT CAST(geo_col AS VARBINARY) FROM iceberg0.unpartitioned_db.t_unknown_types"));
        assertContains(ex.getMessage(), "Datatype of external table column [geo_col] is not supported");
    }

    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.doInit(newFolder(temp, "junit").toURI().toString());
    }

    @Test
    public void testSelectUnsupportedColumnFails() {
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT geo_col FROM iceberg0.unpartitioned_db.t_unknown_types"));
        Assertions.assertTrue(ex.getMessage().contains("Datatype of external table column [geo_col] is not supported"),
                ex.getMessage());
    }

    @Test
    public void testSupportedProjectionStillWorks() throws Exception {
        String plan = getFragmentPlan("SELECT id FROM iceberg0.unpartitioned_db.t_unknown_types");
        Assertions.assertTrue(plan.contains("IcebergScanNode"));
    }

    @Test
    public void testSelectAnotherUnsupportedColumnFails() {
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT ts_nano_col FROM iceberg0.unpartitioned_db.t_unknown_types"));
        Assertions.assertTrue(ex.getMessage().contains("not supported"),
                "Expected 'not supported' error, got: " + ex.getMessage());
    }

    @Test
    public void testSelectStarWithUnsupportedColumnFails() {
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT * FROM iceberg0.unpartitioned_db.t_unknown_types"));
        Assertions.assertTrue(ex.getMessage().contains("not supported"),
                "Expected 'not supported' error, got: " + ex.getMessage());
    }

}
