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

    LOAD_MGR(2),

    ALTER_MGR(3),

    PLUGIN_MGR(4),

    DELETE_MGR(5),

    ANALYZE_MGR(6),

    RESOURCE_GROUP_MGR(7),

    ROUTINE_LOAD_MGR(8),

    GLOBAL_TRANSACTION_MGR(9),

    @Deprecated
    AUTH(10),

    AUTHENTICATION_MGR(11),

    AUTHORIZATION_MGR(12),

    EXPORT_MGR(13),

    SMALL_FILE_MGR(14),

    CATALOG_MGR(15),

    INSERT_OVERWRITE_JOB_MGR(16),

    COMPACTION_MGR(17),

    STREAM_LOAD_MGR(18),

    MATERIALIZED_VIEW_MGR(19),

    GLOBAL_FUNCTION_MGR(20),

    BACKUP_MGR(21),

    LOCAL_META_STORE(22),

    CATALOG_RECYCLE_BIN(23),

    COLOCATE_TABLE_INDEX(24),

    VARIABLE_MGR(25);

    @SerializedName("i")
    private final int id;

    SRMetaBlockID(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}
