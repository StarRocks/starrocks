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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.catalog.Column;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.GetUnfilteredTableMetadataResponse;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeFormationSchemaProjectionTest {

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf", "123456789012", "us-west-2", "db", "t");

    private static Column col(String name) {
        return new Column(name, IntegerType.INT);
    }

    /**
     * Built through the real value object so that hasAuthorizedColumns() carries its true two-state meaning.
     * Constructing the metadata by hand would make the "field absent" case untestable, and that distinction
     * is the whole point of this class.
     */
    private static AuthorizedTableMetadata authorizing(String... columns) {
        return AuthorizedTableMetadata.from(GetUnfilteredTableMetadataResponse.builder()
                .authorizedColumns(columns)
                .build());
    }

    private static AuthorizedTableMetadata withoutAuthorizedColumnsField() {
        return AuthorizedTableMetadata.from(GetUnfilteredTableMetadataResponse.builder().build());
    }

    private static List<String> names(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }

    @Test
    public void testProjectsByNameNotByPosition() {
        List<Column> physical = List.of(col("id"), col("region"), col("ssn"));
        // Lake Formation lists them in its own order; the result must follow the physical one.
        List<Column> projected =
                LakeFormationSchemaProjection.project(physical, authorizing("ssn", "id"), IDENTITY);
        assertEquals(List.of("id", "ssn"), names(projected));
    }

    @Test
    public void testColumnNameMatchIsCaseInsensitive() {
        List<Column> physical = List.of(col("Id"), col("Region"));
        List<Column> projected =
                LakeFormationSchemaProjection.project(physical, authorizing("id", "REGION"), IDENTITY);
        assertEquals(List.of("Id", "Region"), names(projected));
    }

    @Test
    public void testAbsentAuthorizedColumnsFieldFailsClosed() {
        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationSchemaProjection.project(
                        List.of(col("id")), withoutAuthorizedColumnsField(), IDENTITY));
        assertTrue(e.getMessage().contains("no AuthorizedColumns field"), e.getMessage());
    }

    /** A different answer from "the field was absent", so it must not share its message. */
    @Test
    public void testAuthorizedZeroColumnsFailsClosedWithADifferentMessage() {
        LakeFormationTableAccessException zero = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationSchemaProjection.project(List.of(col("id")), authorizing(), IDENTITY));
        LakeFormationTableAccessException absent = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationSchemaProjection.project(
                        List.of(col("id")), withoutAuthorizedColumnsField(), IDENTITY));

        assertTrue(zero.getMessage().contains("no SELECT grant on any column"), zero.getMessage());
        assertNotEquals(absent.getMessage(), zero.getMessage());
    }

    @Test
    public void testAuthorizedColumnMissingFromPhysicalSchemaFailsClosed() {
        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationSchemaProjection.project(
                        List.of(col("id"), col("region")), authorizing("id", "ghost"), IDENTITY));
        assertTrue(e.getMessage().contains("ghost"), e.getMessage());
        assertTrue(e.getMessage().contains("do not exist in the physical schema"), e.getMessage());
    }

    /** Case insensitive matching makes these two the same column, so the intersection is undefined. */
    @Test
    public void testDuplicatePhysicalColumnNamesFailClosed() {
        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationSchemaProjection.project(
                        List.of(col("Id"), col("id")), authorizing("id"), IDENTITY));
        assertTrue(e.getMessage().contains("differ only in case"), e.getMessage());
        // The duplicated name is not echoed back: the caller may have no grant on it.
        assertFalse(e.getMessage().contains("'Id'"), e.getMessage());
    }

    @Test
    public void testAuthorizingEveryColumnKeepsTheWholeSchema() {
        List<Column> physical = List.of(col("id"), col("region"));
        assertEquals(List.of("id", "region"),
                names(LakeFormationSchemaProjection.project(physical, authorizing("id", "region"), IDENTITY)));
    }
}
