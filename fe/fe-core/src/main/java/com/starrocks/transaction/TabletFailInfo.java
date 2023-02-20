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


package com.starrocks.transaction;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.starrocks.common.io.Writable;
import com.starrocks.thrift.TTabletFailInfo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class TabletFailInfo implements Writable {

    private long tabletId;
    private long backendId;

    public TabletFailInfo(long tabletId, long backendId) {
        super();
        this.tabletId = tabletId;
        this.backendId = backendId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBackendId() {
        return backendId;
    }

    public static List<TabletFailInfo> fromThrift(List<TTabletFailInfo> tTabletFailInfos) {
        List<TabletFailInfo> failInfos = Lists.newArrayList();
        if (tTabletFailInfos != null) {
            for (TTabletFailInfo tTabletFailInfo : tTabletFailInfos) {
                failInfos.add(new TabletFailInfo(tTabletFailInfo.getTabletId(),
                        tTabletFailInfo.getBackendId()));

            }
        }
        return failInfos;
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
}
