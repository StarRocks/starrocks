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

package com.starrocks.context;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Validation tests for the payload-shape contract of the three new daemon handlers
 * ({@code DERIVED_PAGE}, {@code REFERENCE_RESYNC}, {@code WORKSPACE_COMMIT}). End-to-end
 * pipelines run inside {@code ContextTaskDaemonE2ETest}; this file exercises only the
 * argument-validation branches that each handler runs before touching SQL — so we can pin
 * those error paths without booting a full UT cluster.
 */
public class ContextTaskDaemonHandlersTest {

    @Test
    public void derivedPageRejectsEmptyPayload() {
        ContextTaskDaemon d = new ContextTaskDaemon(ContextTaskScheduler.TaskKind.DERIVED_PAGE);
        Throwable t = Assertions.assertThrows(IllegalArgumentException.class,
                () -> d.handleDerivedPage(1L, 0L, ""));
        Assertions.assertTrue(t.getMessage().contains("empty payload"));
    }

    @Test
    public void derivedPageRejectsMissingFields() {
        ContextTaskDaemon d = new ContextTaskDaemon(ContextTaskScheduler.TaskKind.DERIVED_PAGE);
        // Missing source_entity_ids
        Throwable t = Assertions.assertThrows(IllegalArgumentException.class,
                () -> d.handleDerivedPage(1L, 0L,
                        "{\"contextbase\":\"cb\",\"collection\":\"col\"}"));
        Assertions.assertTrue(t.getMessage().contains("source_entity_ids"));

        // Empty source_entity_ids list
        Throwable t2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> d.handleDerivedPage(1L, 0L,
                        "{\"contextbase\":\"cb\",\"collection\":\"col\",\"source_entity_ids\":[]}"));
        Assertions.assertTrue(t2.getMessage().contains("source_entity_ids"));
    }

    @Test
    public void referenceResyncRequiresEntityId() {
        ContextTaskDaemon d = new ContextTaskDaemon(ContextTaskScheduler.TaskKind.REFERENCE_RESYNC);

        Throwable t1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> d.handleReferenceResync(1L, 0L, ""));
        Assertions.assertTrue(t1.getMessage().contains("empty payload"));

        Throwable t2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> d.handleReferenceResync(1L, 0L, "{}"));
        Assertions.assertTrue(t2.getMessage().contains("entity_id"));
    }

    @Test
    public void workspaceCommitRequiresWorkspaceField() {
        ContextTaskDaemon d = new ContextTaskDaemon(ContextTaskScheduler.TaskKind.WORKSPACE_COMMIT);

        Throwable t1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> d.handleWorkspaceCommit(1L, 0L, ""));
        Assertions.assertTrue(t1.getMessage().contains("empty payload"));

        Throwable t2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> d.handleWorkspaceCommit(1L, 0L, "{}"));
        Assertions.assertTrue(t2.getMessage().contains("workspace"));
    }
}
