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
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.PermissionTypeMismatchException;

import static org.junit.jupiter.api.Assertions.assertFalse;
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

    /** What a metadata enumeration may leave out, and what it may not. */
    @Test
    public void testOnlyThePerTableFailuresAreSomethingAnEnumerationMaySkip() {
        LakeFormationTableIdentity identity =
                new LakeFormationTableIdentity("lf", null, "us-east-2", "db", "t");

        assertTrue(LakeFormationErrors.metadataFailure(identity,
                AccessDeniedException.builder().message("no grant").build()).isNothingToDescribe());
        assertTrue(LakeFormationErrors.metadataFailure(identity,
                EntityNotFoundException.builder().message("gone").build()).isNothingToDescribe());

        assertFalse(LakeFormationErrors.metadataFailure(identity,
                        PermissionTypeMismatchException.builder().message("filters").build())
                .isNothingToDescribe(), "a contract drift has to fail the statement");
        assertFalse(LakeFormationErrors.metadataFailure(identity, new RuntimeException("AWS is down"))
                .isNothingToDescribe(), "an unclassified failure could be all of AWS, not one table");
    }

    /** And the classification survives the rewrap every resolution does on its way out. */
    @Test
    public void testTheClassificationSurvivesARewrap() {
        LakeFormationTableAccessException skippable =
                LakeFormationTableAccessException.nothingToDescribe("nothing here");
        assertTrue(new LakeFormationTableAccessException(skippable.getMessage(), skippable)
                .isNothingToDescribe());

        LakeFormationTableAccessException attemptLevel =
                new LakeFormationTableAccessException("the budget ran out");
        assertFalse(new LakeFormationTableAccessException(attemptLevel.getMessage(), attemptLevel)
                .isNothingToDescribe());
    }
}
