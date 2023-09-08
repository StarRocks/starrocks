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
package com.starrocks.connector.delta.cache;

import com.starrocks.connector.delta.CachingDeltaLakeMetadata;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.expressions.Expression;

import java.util.List;

public class CachingDeltaSnapshotImpl implements Snapshot {

    private final DeltaLakeTableName tableName;
    private final CachingDeltaLakeMetadata caching;
    private DeltaScan deltaScan;

    public CachingDeltaSnapshotImpl(DeltaLakeTableName tableName, CachingDeltaLakeMetadata caching) {
        this.tableName = tableName;
        this.caching = caching;
    }


    @Override
    public DeltaScan scan() {
        this.deltaScan = new CachingDeltaScanImpl(tableName, caching);
        return deltaScan;
    }

    @Override
    public DeltaScan scan(Expression predicate) {
        this.deltaScan = new CachingDeltaScanImpl(tableName, caching, predicate);
        return deltaScan;
    }

    @Override
    public List<AddFile> getAllFiles() {
        try {
            return caching.getAllFiles(tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Metadata getMetadata() {

        try {
            return caching.getMetadata(tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getVersion() {
        return caching.getCacheSnapshotVersion(tableName);
    }


    //TODO fengyuanshen
    @Override
    public CloseableIterator<RowRecord> open() {
        return null;
    }

}
