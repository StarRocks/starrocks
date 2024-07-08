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


package com.starrocks.sql.common;

import java.util.Map;

/**
 * Diff result of list partitions.
 */
public final class ListPartitionDiff extends PartitionDiff {

    Map<String, PListCell> adds;

    Map<String, PListCell> deletes;

    public ListPartitionDiff(Map<String, PListCell> adds, Map<String, PListCell> deletes) {
        this.adds = adds;
        this.deletes = deletes;
    }

    public Map<String, PListCell> getAdds() {
        return adds;
    }

    public void setAdds(Map<String, PListCell> adds) {
        this.adds = adds;
    }

    public Map<String, PListCell> getDeletes() {
        return deletes;
    }

    public void setDeletes(Map<String, PListCell> deletes) {
        this.deletes = deletes;
    }
}
