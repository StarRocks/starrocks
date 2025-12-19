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

import java.util.List;

/**
 * Holds cached manifest lists for an Iceberg snapshot.
 * This avoids re-reading and re-parsing the manifest-list file (snap-*.avro)
 * on every query to the same snapshot.
 */
public class SnapshotManifests {
    private final List<ManifestFile> dataManifests;
    private final List<ManifestFile> deleteManifests;

    public SnapshotManifests(List<ManifestFile> dataManifests, List<ManifestFile> deleteManifests) {
        this.dataManifests = dataManifests;
        this.deleteManifests = deleteManifests;
    }

    public List<ManifestFile> getDataManifests() {
        return dataManifests;
    }

    public List<ManifestFile> getDeleteManifests() {
        return deleteManifests;
    }
}
