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

import java.util.Objects;

public class SRMetaBlockID {
    @SerializedName("i")
    protected int id;

    protected SRMetaBlockID(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }


    public static final SRMetaBlockID INVALID = new SRMetaBlockID(0);

    public static final SRMetaBlockID NODE_MGR = new SRMetaBlockID(1);

    public static final SRMetaBlockID LOCAL_META_STORE = new SRMetaBlockID(2);

    public static final SRMetaBlockID ALTER_MGR = new SRMetaBlockID(3);

    public static final SRMetaBlockID CATALOG_RECYCLE_BIN = new SRMetaBlockID(4);

    public static final SRMetaBlockID VARIABLE_MGR = new SRMetaBlockID(5);

    public static final SRMetaBlockID RESOURCE_MGR = new SRMetaBlockID(6);

    public static final SRMetaBlockID EXPORT_MGR = new SRMetaBlockID(7);

    public static final SRMetaBlockID BACKUP_MGR = new SRMetaBlockID(8);

    @Deprecated
    public static final SRMetaBlockID AUTH = new SRMetaBlockID(9);

    public static final SRMetaBlockID GLOBAL_TRANSACTION_MGR = new SRMetaBlockID(10);

    public static final SRMetaBlockID COLOCATE_TABLE_INDEX = new SRMetaBlockID(11);

    public static final SRMetaBlockID ROUTINE_LOAD_MGR = new SRMetaBlockID(12);

    public static final SRMetaBlockID LOAD_MGR = new SRMetaBlockID(13);

    public static final SRMetaBlockID SMALL_FILE_MGR = new SRMetaBlockID(14);

    public static final SRMetaBlockID PLUGIN_MGR = new SRMetaBlockID(15);

    public static final SRMetaBlockID DELETE_MGR = new SRMetaBlockID(16);

    public static final SRMetaBlockID ANALYZE_MGR = new SRMetaBlockID(17);

    public static final SRMetaBlockID RESOURCE_GROUP_MGR = new SRMetaBlockID(18);

    public static final SRMetaBlockID AUTHENTICATION_MGR = new SRMetaBlockID(19);

    public static final SRMetaBlockID AUTHORIZATION_MGR = new SRMetaBlockID(20);

    public static final SRMetaBlockID TASK_MGR = new SRMetaBlockID(21);

    public static final SRMetaBlockID CATALOG_MGR = new SRMetaBlockID(22);

    public static final SRMetaBlockID INSERT_OVERWRITE_JOB_MGR = new SRMetaBlockID(23);

    public static final SRMetaBlockID COMPACTION_MGR = new SRMetaBlockID(24);

    public static final SRMetaBlockID STREAM_LOAD_MGR = new SRMetaBlockID(25);

    public static final SRMetaBlockID MATERIALIZED_VIEW_MGR = new SRMetaBlockID(26);

    public static final SRMetaBlockID GLOBAL_FUNCTION_MGR = new SRMetaBlockID(27);

    public static final SRMetaBlockID STORAGE_VOLUME_MGR = new SRMetaBlockID(28);

    public static final SRMetaBlockID DICTIONARY_MGR = new SRMetaBlockID(29);

    public static final SRMetaBlockID REPLICATION_MGR = new SRMetaBlockID(30);

    @Override
    public String toString() {
        return String.valueOf(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SRMetaBlockID that = (SRMetaBlockID) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
