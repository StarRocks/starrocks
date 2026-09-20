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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeFormationTableIdentityTest {

    private static LakeFormationTableIdentity identity(String table) {
        return new LakeFormationTableIdentity("lf_catalog", "123456789012", "us-west-2", "db", table);
    }

    /**
     * Every field distinguishes: the same (db, table) pair is a different object in another account or
     * region, so an identity built from (db, table) alone would collide across catalogs.
     */
    @Test
    public void testEveryFieldDistinguishes() {
        assertEquals(identity("t"), identity("t"));
        assertEquals(identity("t").hashCode(), identity("t").hashCode());

        assertNotEquals(identity("t"), identity("other"));
        assertNotEquals(identity("t"),
                new LakeFormationTableIdentity("lf_catalog", "999999999999", "us-west-2", "db", "t"));
        assertNotEquals(identity("t"),
                new LakeFormationTableIdentity("lf_catalog", "123456789012", "us-east-1", "db", "t"));
        assertNotEquals(identity("t"),
                new LakeFormationTableIdentity("other_catalog", "123456789012", "us-west-2", "db", "t"));
        assertNotEquals(identity("t"),
                new LakeFormationTableIdentity("lf_catalog", "123456789012", "us-west-2", "other_db", "t"));
    }

    @Test
    public void testRejectsMissingFields() {
        // awsCatalogId is deliberately absent from this list: it is nullable, and null means the caller's
        // own AWS account - which is exactly what the Glue request sends when the catalog did not name one.

        assertThrows(NullPointerException.class,
                () -> new LakeFormationTableIdentity(null, "123456789012", "us-west-2", "db", "t"));
        assertThrows(NullPointerException.class,
                () -> new LakeFormationTableIdentity("lf_catalog", "123456789012", null, "db", "t"));
    }

    @Test
    public void testToStringNamesTheAccountAndRegion() {
        // Error messages have to say which account and region were consulted, otherwise a
        // cross-account misconfiguration reads like a plain permission problem.
        String text = identity("t").toString();
        assertTrue(text.contains("lf_catalog.db.t"));
        assertTrue(text.contains("123456789012"));
        assertTrue(text.contains("us-west-2"));
    }
}
