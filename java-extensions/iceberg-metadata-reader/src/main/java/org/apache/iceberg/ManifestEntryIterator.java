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

import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * Iterates over all the entries of a manifest, including the DELETED ones, for the $entries metadata table.
 * ManifestReader#entries() and ManifestEntry are package-private, so this class lives in the Iceberg package
 * and exposes the fields of the current entry. The status, snapshot id and sequence numbers include the values
 * inherited from the manifest.
 */
public class ManifestEntryIterator implements Closeable {
    private final CloseableIterator<? extends ManifestEntry<? extends ContentFile<?>>> iterator;
    private ManifestEntry<? extends ContentFile<?>> current;

    public ManifestEntryIterator(ManifestFile manifest, FileIO io, Map<Integer, PartitionSpec> specsById) {
        CloseableIterable<? extends ManifestEntry<? extends ContentFile<?>>> entries;
        if (manifest.content() == ManifestContent.DATA) {
            entries = ManifestFiles.read(manifest, io, specsById).entries();
        } else {
            entries = ManifestFiles.readDeleteManifest(manifest, io, specsById).entries();
        }
        this.iterator = entries.iterator();
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    // Moves to the next entry. The reader reuses the entry and its file, so read them before the next call.
    public void next() {
        current = iterator.next();
    }

    public int status() {
        return current.status().id();
    }

    public Long snapshotId() {
        return current.snapshotId();
    }

    public Long dataSequenceNumber() {
        return current.dataSequenceNumber();
    }

    public Long fileSequenceNumber() {
        return current.fileSequenceNumber();
    }

    public ContentFile<?> file() {
        return current.file();
    }

    @Override
    public void close() throws IOException {
        iterator.close();
    }
}
