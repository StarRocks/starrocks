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

import com.starrocks.connector.exception.StarRocksConnectorException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeFormationTableAccessExceptionTest {

    /**
     * The type is the whole point: fail-closed paths identify a Lake Formation failure by type so it
     * can punch through catch blocks that swallow generic connector exceptions. Being unchecked is
     * what lets it cross the connector SPI, which cannot declare checked exceptions.
     */
    @Test
    public void testIsAnUncheckedConnectorException() {
        LakeFormationTableAccessException e =
                new LakeFormationTableAccessException("hive_catalog.db.tbl is not authorized");
        assertTrue(e instanceof StarRocksConnectorException);
        assertTrue(e instanceof RuntimeException);
        assertTrue(e.getMessage().contains("hive_catalog.db.tbl"));
    }

    @Test
    public void testKeepsItsCause() {
        // The cause has to survive: LazyConnector rethrows this type as-is precisely so diagnosis is
        // not lost behind a generic wrapper.
        IllegalStateException cause = new IllegalStateException("underlying");
        LakeFormationTableAccessException e = new LakeFormationTableAccessException("wrapped", cause);
        assertSame(cause, e.getCause());
    }
}
