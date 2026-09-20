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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.ColumnRowFilter;
import software.amazon.awssdk.services.glue.model.GetUnfilteredTableMetadataResponse;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthorizedTableMetadataTest {

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf_catalog", "123456789012", "us-west-2", "db", "tbl");

    private static GetUnfilteredTableMetadataResponse.Builder response() {
        return GetUnfilteredTableMetadataResponse.builder()
                .table(Table.builder().name("tbl").build())
                .queryAuthorizationId("auth-1");
    }

    /**
     * The distinction this whole type exists for. "Lake Formation said nothing about columns" and
     * "Lake Formation authorized zero columns" are different answers that must not be collapsed:
     * the first is a response we do not understand and has to fail, the second is a real - if
     * useless - authorization. The SDK reports them as hasAuthorizedColumns()=false versus an
     * explicitly empty list, and this asserts the SDK really does behave that way.
     */
    @Test
    public void testAbsentColumnListIsNotAnEmptyOne() {
        AuthorizedTableMetadata absent = AuthorizedTableMetadata.from(
                response().isRegisteredWithLakeFormation(true).build());
        assertFalse(absent.hasAuthorizedColumns());
        assertEquals(List.of(), absent.authorizedColumns());

        AuthorizedTableMetadata empty = AuthorizedTableMetadata.from(
                response().isRegisteredWithLakeFormation(true).authorizedColumns(List.of()).build());
        assertTrue(empty.hasAuthorizedColumns());
        assertEquals(List.of(), empty.authorizedColumns());

        AuthorizedTableMetadata present = AuthorizedTableMetadata.from(
                response().isRegisteredWithLakeFormation(true).authorizedColumns("c1").build());
        assertTrue(present.hasAuthorizedColumns());
        assertEquals(List.of("c1"), present.authorizedColumns());
    }

    /**
     * Only an explicit false may send a table down the ordinary path. An absent flag is a response we
     * do not understand, and reading it as "not registered" would bypass Lake Formation entirely.
     */
    @Test
    public void testRegisteredFlagIsThreeValued() {
        assertTrue(AuthorizedTableMetadata.from(
                response().isRegisteredWithLakeFormation(true).build()).isRegistered(IDENTITY));
        assertFalse(AuthorizedTableMetadata.from(
                response().isRegisteredWithLakeFormation(false).build()).isRegistered(IDENTITY));

        AuthorizedTableMetadata absent = AuthorizedTableMetadata.from(response().build());
        LakeFormationTableAccessException e =
                assertThrows(LakeFormationTableAccessException.class, () -> absent.isRegistered(IDENTITY));
        assertTrue(e.getMessage().contains("refusing to guess"));
        assertTrue(e.getMessage().contains("lf_catalog.db.tbl"));
    }

    /** Row and cell filters are unenforceable in phase 1, so they have to be visible to the caller. */
    @Test
    public void testFiltersAreReported() {
        assertFalse(AuthorizedTableMetadata.from(
                response().isRegisteredWithLakeFormation(true).build()).hasFilters());
        assertTrue(AuthorizedTableMetadata.from(
                response().rowFilter("c1 > 1").build()).hasFilters());
        // Exists but carries no expression: a shape this code cannot read, so it must not be dropped.
        assertTrue(AuthorizedTableMetadata.from(
                response().cellFilters(ColumnRowFilter.builder().columnName("c1").build()).build())
                .hasFilters());
    }

    /**
     * What Lake Formation actually answers for a plain full-table SELECT grant with no filter
     * configured anywhere - observed against a real account, us-east-2:
     *
     * <pre>
     *   RowFilter   = "TRUE"
     *   CellFilters = one entry per authorized column, each RowFilterExpression = "TRUE"
     * </pre>
     *
     * TRUE means "allows everything". Reading the mere presence of these fields as a restriction
     * refused every table in the catalog, which is how this shipped: the mocks all returned empty
     * filters, so nothing exercised the normal path until it ran against AWS.
     */
    @Test
    public void testTautologicalFiltersAreNotRestrictions() {
        assertFalse(AuthorizedTableMetadata.from(
                response().isRegisteredWithLakeFormation(true)
                        .rowFilter("TRUE")
                        .cellFilters(
                                ColumnRowFilter.builder().columnName("id")
                                        .rowFilterExpression("TRUE").build(),
                                ColumnRowFilter.builder().columnName("region")
                                        .rowFilterExpression("TRUE").build())
                        .build()).hasFilters(),
                "a full-table grant must not be read as a row/cell filter");

        // Spelling and padding do not change the meaning.
        assertFalse(AuthorizedTableMetadata.from(
                response().rowFilter(" true ").build()).hasFilters());

        // One restrictive column among tautological ones still restricts.
        assertTrue(AuthorizedTableMetadata.from(
                response().rowFilter("TRUE")
                        .cellFilters(
                                ColumnRowFilter.builder().columnName("id")
                                        .rowFilterExpression("TRUE").build(),
                                ColumnRowFilter.builder().columnName("amount")
                                        .rowFilterExpression("amount < 100").build())
                        .build()).hasFilters());

        // An empty expression is not TRUE, so it fails closed rather than being waved through.
        assertTrue(AuthorizedTableMetadata.from(
                response().rowFilter("").build()).hasFilters());
    }

    @Test
    public void testAuthorizationIdIsNotInToString() {
        AuthorizedTableMetadata metadata = AuthorizedTableMetadata.from(
                response().isRegisteredWithLakeFormation(true).queryAuthorizationId("SECRET").build());
        assertEquals("SECRET", metadata.queryAuthorizationId());
        assertFalse(metadata.toString().contains("SECRET"));
    }

    @Test
    public void testColumnListIsImmutable() {
        AuthorizedTableMetadata metadata = AuthorizedTableMetadata.from(
                response().isRegisteredWithLakeFormation(true).authorizedColumns("c1").build());
        assertThrows(UnsupportedOperationException.class, () -> metadata.authorizedColumns().add("c2"));
    }

    /**
     * The two filters are read back verbatim rather than folded into hasFilters(): the refusal message
     * names which kind arrived, and a caller that could only ask "are there filters" could not say it.
     */
    @Test
    public void testBothFilterKindsAreReadBackAsTheyArrived() {
        ColumnRowFilter cellFilter = ColumnRowFilter.builder()
                .columnName("amount").rowFilterExpression("amount < 100").build();
        AuthorizedTableMetadata metadata = AuthorizedTableMetadata.from(
                response().isRegisteredWithLakeFormation(true)
                        .rowFilter("region = 'us'")
                        .cellFilters(cellFilter)
                        .build());
        assertEquals("region = 'us'", metadata.rowFilter());
        assertEquals(List.of(cellFilter), metadata.cellFilters());
        assertTrue(metadata.hasFilters());
    }
}
