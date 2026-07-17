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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

// Journal entry for physically erasing a materialized index that was parked in the
// CatalogRecycleBin (e.g. a superseded index retired by a tablet reshard). Keyed by the
// index's own (globally unique) id.
public class EraseMaterializedIndexLog implements Writable {

    @SerializedName(value = "indexId")
    private long indexId;

    public EraseMaterializedIndexLog(long indexId) {
        this.indexId = indexId;
    }

    public long getIndexId() {
        return indexId;
    }
}
