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

import com.starrocks.catalog.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The handover between a statement's two resolutions of one table. What is pinned here is the key: an
 * authorized table is reachable from the statement that authorized it, and from nothing else.
 */
public class LakeFormationPlannedTablesTest {

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf", null, "us-west-2", "db", "t");
    private static final LakeFormationTableIdentity OTHER_TABLE =
            new LakeFormationTableIdentity("lf", null, "us-west-2", "db", "other");

    private static Table table(String name) {
        return new Table(1L, name, Table.TableType.HIVE, List.of());
    }

    @AfterEach
    public void tearDown() {
        LakeFormationPlannedTables.forgetAll();
    }

    @Test
    public void testAStatementFindsTheTableItAuthorized() {
        Table authorized = table("t");
        LakeFormationPlannedTables.remember("q1", IDENTITY, authorized);

        assertSame(authorized, LakeFormationPlannedTables.find("q1", IDENTITY));
    }

    /**
     * The whole point of keying by query id. ANALYZE resolves its table a second time after planning ended,
     * and what it gets back has to be the table its own statement was authorized for - never one another
     * statement left behind, which carries a different user's authorized columns.
     */
    @Test
    public void testAnotherStatementFindsNothing() {
        LakeFormationPlannedTables.remember("q1", IDENTITY, table("t"));

        assertNull(LakeFormationPlannedTables.find("q2", IDENTITY),
                "one statement must not reach what another authorized");
    }

    /** A thread with no query of its own - a cache loader, a follower replaying a journal - finds nothing. */
    @Test
    public void testAnUnknownStatementFindsNothing() {
        assertNull(LakeFormationPlannedTables.find("never-planned", IDENTITY));
    }

    @Test
    public void testOneStatementsTablesAreKeptApart() {
        Table first = table("t");
        Table second = table("other");
        LakeFormationPlannedTables.remember("q1", IDENTITY, first);
        LakeFormationPlannedTables.remember("q1", OTHER_TABLE, second);

        assertSame(first, LakeFormationPlannedTables.find("q1", IDENTITY));
        assertSame(second, LakeFormationPlannedTables.find("q1", OTHER_TABLE));
    }

    @Test
    public void testForgettingLeavesNothingBehind() {
        LakeFormationPlannedTables.remember("q1", IDENTITY, table("t"));
        LakeFormationPlannedTables.forgetAll();

        assertNull(LakeFormationPlannedTables.find("q1", IDENTITY));
    }
}
