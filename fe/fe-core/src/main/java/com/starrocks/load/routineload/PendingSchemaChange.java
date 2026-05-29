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

package com.starrocks.load.routineload;

import com.starrocks.thrift.TColumn;

import java.util.List;

// Runtime-only record that a routine-load task hit an Avro writer schema the table does not yet cover. Set
// on the job by RoutineLoadJob.afterAborted from the BE rollback attachment and cleared by the
// schema-evolution daemon once the resulting ALTER resolves. While a job holds one, the task scheduler
// defers dispatching its tasks, so they do not re-abort against the not-yet-evolved table.
//
// The BE ships the full writer schema, not a diff: schemaColumns carries every top-level field (name + full
// nested type, nullable, no default); the FE diffs it against the live table and picks ADD COLUMN / ADD
// FIELD / MODIFY. widenColumns names existing varchar/varbinary columns an over-length value overran (widen
// to the type's max length). The two are independent; either may be empty.
//
// Not persisted: after a leader failover this is simply re-derived from the next BE signal.
public class PendingSchemaChange {
    private final int schemaId;
    private final List<TColumn> schemaColumns;
    private final List<String> widenColumns;
    // Set once the daemon has applied the ALTER. On a later tick, if the table still does not cover the
    // schema and no ALTER is running, the change is treated as failed (job paused) rather than re-applied,
    // so a cancelled/ineffective ALTER cannot loop.
    private boolean applied = false;

    public PendingSchemaChange(int schemaId, List<TColumn> schemaColumns, List<String> widenColumns) {
        this.schemaId = schemaId;
        this.schemaColumns = schemaColumns;
        this.widenColumns = widenColumns;
    }

    public boolean isApplied() {
        return applied;
    }

    public void setApplied(boolean applied) {
        this.applied = applied;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public List<TColumn> getSchemaColumns() {
        return schemaColumns;
    }

    public List<String> getWidenColumns() {
        return widenColumns;
    }
}
