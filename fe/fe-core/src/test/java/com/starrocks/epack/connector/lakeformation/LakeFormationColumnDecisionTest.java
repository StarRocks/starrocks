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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.connector.TableLoadPurpose;
import com.starrocks.epack.authorization.NativeAccessControllerEPack;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The column decision itself, which the delegation test in LakeFormationAccessControllerTest excludes on
 * purpose. Nothing covered this path before, and that is how a false denial of every count(*) shipped.
 */
public class LakeFormationColumnDecisionTest {

    private static final TableName TABLE = new TableName("lf", "db", "t");
    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf", null, "us-east-2", "db", "t");

    /** Physical: id, region, amount, customer_email + partition key dt. Authorized: id, region, amount. */
    private static LakeFormationHiveTable authorizedTable() {
        List<Column> physical = new ArrayList<>();
        physical.add(new Column("id", IntegerType.INT));
        physical.add(new Column("region", IntegerType.INT));
        physical.add(new Column("amount", IntegerType.INT));
        physical.add(new Column("customer_email", IntegerType.INT));
        physical.add(new Column("dt", IntegerType.INT));

        HiveTable table = HiveTable.builder()
                .setId(1L)
                .setTableName("t")
                .setCatalogName("lf")
                .setHiveDbName("db")
                .setHiveTableName("t")
                .setTableLocation("s3://bucket/db/t")
                .setCreateTime(1600000000L)
                .setFullSchema(physical)
                .setPartitionColumnNames(new ArrayList<>(List.of("dt")))
                .setDataColumnNames(new ArrayList<>(List.of("id", "region", "amount", "customer_email")))
                .setProperties(new HashMap<>())
                .setSerdeProperties(new HashMap<>())
                .build();

        List<Column> authorized = new ArrayList<>();
        authorized.add(physical.get(0));
        authorized.add(physical.get(1));
        authorized.add(physical.get(2));
        return LakeFormationHiveTable.of(table, authorized, IDENTITY,
                new LakeFormationTableHandle(IDENTITY, TableLoadPurpose.DATA_ACCESS, "attempt-1"));
    }

    private LakeFormationAccessController controller;

    @BeforeEach
    public void setUp() {
        LakeFormationHiveTable table = authorizedTable();
        new MockUp<MetadataMgr>() {
            @Mock
            public Table getTable(ConnectContext context, String catalog, String db, String tbl) {
                return table;
            }
        };
        // The native table level decision is a separate concern; let it pass so the column rule is what
        // this test observes.
        new MockUp<NativeAccessControllerEPack>() {
            @Mock
            public void checkTableAction(ConnectContext context, TableName tableName,
                                         PrivilegeType privilegeType) {
            }
        };
        controller = new LakeFormationAccessController(new NativeAccessControllerEPack());
    }

    private void check(String column) throws AccessDeniedException {
        controller.checkColumnAction(new ConnectContext(), TABLE, column, PrivilegeType.SELECT);
    }

    @Test
    public void testAuthorizedColumnIsAllowedRegardlessOfCase() {
        assertDoesNotThrow(() -> check("id"));
        assertDoesNotThrow(() -> check("ID"));
    }

    /** The decision that matters: a real column Lake Formation did not authorize. */
    @Test
    public void testUnauthorizedPhysicalColumnIsRefused() {
        AccessDeniedException e = assertThrows(AccessDeniedException.class, () -> check("customer_email"));
        assertTrue(e.getMessage().contains("customer_email"), e.getMessage());
    }

    /**
     * And it stays refused in another case. The physical-column test is case insensitive on purpose: were
     * it not, CUSTOMER_EMAIL would look synthetic and be waved through.
     */
    @Test
    public void testUnauthorizedPhysicalColumnIsRefusedInAnotherCase() {
        assertThrows(AccessDeniedException.class, () -> check("CUSTOMER_EMAIL"));
        assertThrows(AccessDeniedException.class, () -> check("Customer_Email"));
    }

    /** A partition key that was not authorized is a physical column, so it is refused like any other. */
    @Test
    public void testUnauthorizedPartitionColumnIsRefused() {
        assertThrows(AccessDeniedException.class, () -> check("dt"));
    }

    /** The regression this exists for. */
    @Test
    public void testSyntheticPlaceholderColumnIsNotAnAuthorizationQuestion() {
        assertDoesNotThrow(() -> check("___count___"));
    }

    /** Every other non-physical name is refused, which is a change from how this used to behave. */
    @Test
    public void testOtherSyntheticNamesAreRefused() {
        assertThrows(AccessDeniedException.class, () -> check("___COUNT___"));
        assertThrows(AccessDeniedException.class, () -> check("___min___"));
        assertThrows(AccessDeniedException.class, () -> check("no_such_column"));
    }

    /**
     * And the reason the placeholder is matched by name rather than by "is it physical": a real column that
     * happens to be called ___count___ is real, and an unauthorized real column is refused.
     */
    @Test
    public void testPhysicalColumnNamedLikeThePlaceholderIsStillRefused() {
        LakeFormationHiveTable table = tableWithPhysicalPlaceholderColumn();
        new MockUp<MetadataMgr>() {
            @Mock
            public Table getTable(ConnectContext context, String catalog, String db, String tbl) {
                return table;
            }
        };
        assertThrows(AccessDeniedException.class, () -> check("___count___"));
    }

    /** id and a physical column literally named ___count___; only id is authorized. */
    private static LakeFormationHiveTable tableWithPhysicalPlaceholderColumn() {
        List<Column> physical = new ArrayList<>();
        physical.add(new Column("id", IntegerType.INT));
        physical.add(new Column("___count___", IntegerType.INT));

        HiveTable table = HiveTable.builder()
                .setId(2L)
                .setTableName("t")
                .setCatalogName("lf")
                .setHiveDbName("db")
                .setHiveTableName("t")
                .setTableLocation("s3://bucket/db/t")
                .setCreateTime(1600000000L)
                .setFullSchema(physical)
                .setPartitionColumnNames(new ArrayList<>())
                .setDataColumnNames(new ArrayList<>(List.of("id", "___count___")))
                .setProperties(new HashMap<>())
                .setSerdeProperties(new HashMap<>())
                .build();

        return LakeFormationHiveTable.of(table, new ArrayList<>(List.of(physical.get(0))), IDENTITY,
                new LakeFormationTableHandle(IDENTITY, TableLoadPurpose.DATA_ACCESS, "attempt-1"));
    }
}
