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

package org.apache.iceberg;

import org.apache.iceberg.util.CharSequenceSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * RewriteFiles for the equality-delete -> position-delete conversion procedure: it removes the
 * converted equality-delete files and adds the materialized position-delete files, and -- unlike a
 * plain RewriteFiles -- validates, inside the commit's optimistic-retry loop, that the data files the
 * new position deletes reference are still live in the table.
 *
 * <p>The stock RewriteFiles only validates data files it replaces, and conversion replaces none, so
 * without this a concurrent rewrite could drop a referenced data file between planning and commit and
 * leave the new position deletes pointing at a file that is no longer in the table (resurrecting the
 * rows the equality delete used to hide). Putting the check in {@link #validate} runs it on every
 * retry against the current table state, which a pre-commit check cannot do.
 *
 * <p>Lives in {@code org.apache.iceberg} to reach the package-private {@link BaseRewriteFiles}
 * constructor and the protected {@code validateDataFilesExist} hook.
 */
public class ConvertEqualityDeleteRewriteFiles extends BaseRewriteFiles {
    private final CharSequenceSet referencedDataFiles;
    private final Long startingSnapshotId;

    public ConvertEqualityDeleteRewriteFiles(String tableName, TableOperations ops,
                                             Set<String> referencedDataFiles, Long startingSnapshotId) {
        super(tableName, ops);
        // CharSequenceSet.of in Iceberg 1.10 takes Iterable<CharSequence> (invariant), so a Set<String>
        // cannot be passed directly; copy into a List<CharSequence> first.
        List<CharSequence> paths = new ArrayList<>(referencedDataFiles);
        this.referencedDataFiles = CharSequenceSet.of(paths);
        this.startingSnapshotId = startingSnapshotId;
        if (startingSnapshotId != null) {
            validateFromSnapshot(startingSnapshotId);
        }
    }

    @Override
    protected void validate(TableMetadata base, Snapshot parent) {
        super.validate(base, parent);
        if (parent != null && startingSnapshotId != null && !referencedDataFiles.isEmpty()) {
            // skipDeletes=false: fail if a referenced data file was removed by any of OVERWRITE / REPLACE
            // / DELETE since the planning snapshot, not just by a data-file rewrite.
            validateDataFilesExist(base, startingSnapshotId, referencedDataFiles, false, null, parent);
        }
    }
}
