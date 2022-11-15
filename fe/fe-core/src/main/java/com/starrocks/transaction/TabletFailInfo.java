// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
