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
package com.starrocks.persist.metablock;

import com.google.gson.annotations.SerializedName;

public enum SRMetaBlockID {
    INVALID(0),

    NODE_MGR(1),

    LOCAL_META_STORE(2),

    ALTER_MGR(3),

    CATALOG_RECYCLE_BIN(4),

    VARIABLE_MGR(5),

    RESOURCE_MGR(6),

    EXPORT_MGR(7),

    BACKUP_MGR(8),

    @Deprecated
    AUTH(9),

    GLOBAL_TRANSACTION_MGR(10),

    COLOCATE_TABLE_INDEX(11),

    ROUTINE_LOAD_MGR(12),

    LOAD_MGR(13),

    SMALL_FILE_MGR(14),

    PLUGIN_MGR(15),

    DELETE_MGR(16),

    ANALYZE_MGR(17),

    RESOURCE_GROUP_MGR(18),

    AUTHENTICATION_MGR(19),

    AUTHORIZATION_MGR(20),

    TASK_MGR(21),

    CATALOG_MGR(22),

    INSERT_OVERWRITE_JOB_MGR(23),

    COMPACTION_MGR(24),

    STREAM_LOAD_MGR(25),

    MATERIALIZED_VIEW_MGR(26),

    GLOBAL_FUNCTION_MGR(27);

    @SerializedName("i")
    private final int id;

    SRMetaBlockID(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}
