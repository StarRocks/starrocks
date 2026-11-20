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

package com.starrocks.connector.iceberg;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

// The manifests of every snapshot, with the snapshot that lists each in reference_snapshot_id. A manifest shared by
// several snapshots appears once per snapshot.
public class IcebergAllManifestsTableScanner extends IcebergManifestsTableScanner {
    private long referenceSnapshotId;

    public IcebergAllManifestsTableScanner(int fetchSize, Map<String, String> params) {
        super(fetchSize, params);
    }

    @Override
    protected void initReader() {
        Iterator<Snapshot> snapshots = table.snapshots().iterator();
        reader = new Iterator<>() {
            private Iterator<ManifestFile> manifests = Collections.emptyIterator();
            private long manifestsSnapshotId;

            @Override
            public boolean hasNext() {
                // read the manifest list of one snapshot at a time
                while (!manifests.hasNext() && snapshots.hasNext()) {
                    Snapshot snapshot = snapshots.next();
                    manifestsSnapshotId = snapshot.snapshotId();
                    manifests = snapshot.allManifests(fileIO).iterator();
                }
                return manifests.hasNext();
            }

            @Override
            public ManifestFile next() {
                hasNext();
                referenceSnapshotId = manifestsSnapshotId;
                return manifests.next();
            }
        };
    }

    @Override
    protected Object get(String columnName, ManifestFile file) {
        if (columnName.equals("reference_snapshot_id")) {
            return referenceSnapshotId;
        }
        return super.get(columnName, file);
    }
}
