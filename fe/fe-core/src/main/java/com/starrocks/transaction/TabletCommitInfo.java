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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/TabletCommitInfo.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.transaction;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.starrocks.common.io.Writable;
import com.starrocks.thrift.TTabletCommitInfo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import javax.validation.constraints.NotNull;

public class TabletCommitInfo implements Writable {

    private long tabletId;
    private long backendId;

    // For low cardinality string column with global dict
    private List<String> invalidDictCacheColumns = Lists.newArrayList();
    private List<String> validDictCacheColumns = Lists.newArrayList();
    private List<Long> validDictCollectedVersions = Lists.newArrayList();

    public TabletCommitInfo(long tabletId, long backendId) {
        super();
        this.tabletId = tabletId;
        this.backendId = backendId;
    }

    public TabletCommitInfo(long tabletId, long backendId, List<String> invalidDictCacheColumns,
                            List<String> validDictCacheColumns, List<Long> validDictCollectedVersions) {
        this.tabletId = tabletId;
        this.backendId = backendId;
        this.invalidDictCacheColumns = invalidDictCacheColumns;
        this.validDictCacheColumns = validDictCacheColumns;
        this.validDictCollectedVersions = validDictCollectedVersions;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBackendId() {
        return backendId;
    }

    public List<String> getInvalidDictCacheColumns() {
        return invalidDictCacheColumns;
    }

    public List<String> getValidDictCacheColumns() {
        return validDictCacheColumns;
    }

    public List<Long> getValidDictCollectedVersions() {
        return validDictCollectedVersions;
    }

    @NotNull
    public static List<TabletCommitInfo> fromThrift(List<TTabletCommitInfo> tTabletCommitInfos) {
        List<TabletCommitInfo> commitInfos = Lists.newArrayList();
        if (tTabletCommitInfos == null) {
            return commitInfos;
        }
        for (TTabletCommitInfo tTabletCommitInfo : tTabletCommitInfos) {
            if (tTabletCommitInfo.isSetInvalid_dict_cache_columns()) {
                commitInfos.add(new TabletCommitInfo(tTabletCommitInfo.getTabletId(),
                        tTabletCommitInfo.getBackendId(),
                        tTabletCommitInfo.getInvalid_dict_cache_columns(),
                        tTabletCommitInfo.getValid_dict_cache_columns(),
                        tTabletCommitInfo.getValid_dict_collected_versions()
                ));
            } else {
                commitInfos.add(new TabletCommitInfo(tTabletCommitInfo.getTabletId(),
                        tTabletCommitInfo.getBackendId()));
            }

        }
        return commitInfos;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tabletId);
        out.writeLong(backendId);
    }

    public void readFields(DataInput in) throws IOException {
        tabletId = in.readLong();
        backendId = in.readLong();
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(tabletId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TabletCommitInfo)) {
            return false;
        }

        TabletCommitInfo info = (TabletCommitInfo) obj;
        return (tabletId == info.tabletId) && (backendId == info.backendId);
    }
}
