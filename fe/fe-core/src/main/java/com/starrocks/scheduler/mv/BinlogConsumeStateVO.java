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


package com.starrocks.scheduler.mv;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TBinlogOffset;
import com.starrocks.thrift.TBinlogScanRange;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Binlog group consumption state of MV
 * NOTE: not thread-safe for updating
 */
public class BinlogConsumeStateVO implements Writable {
    @SerializedName("binlogMap")
    private Map<BinlogIdVO, BinlogLSNVO> binlogMap = new HashMap<>();

    public List<TBinlogScanRange> toThrift() {
        List<TBinlogScanRange> res = new ArrayList<>();
        TabletInvertedIndex tabletIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        binlogMap.forEach((key, value) -> {
            TBinlogScanRange scan = new TBinlogScanRange();
            TabletMeta meta = tabletIndex.getTabletMeta(key.getTabletId());
            scan.setTable_id(meta.getTableId());
            scan.setTablet_id(key.getTabletId());
            scan.setPartition_id(meta.getPartitionId());
            scan.setOffset(value.toThrift());
            res.add(scan);
        });
        return res;
    }

    public Map<BinlogIdVO, BinlogLSNVO> getBinlogMap() {
        return binlogMap;
    }

    public static BinlogConsumeStateVO read(DataInput input) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(input), BinlogConsumeStateVO.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BinlogConsumeStateVO that = (BinlogConsumeStateVO) o;
        return Objects.equals(binlogMap, that.binlogMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(binlogMap);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("BinlogConsumeStateVO{");
        for (Map.Entry<BinlogIdVO, BinlogLSNVO> entry : binlogMap.entrySet()) {
            stringBuilder.append("(")
                    .append(entry.getKey().toString()).append(", ")
                    .append(entry.getValue().toString()).append(")");
        }
        stringBuilder.append('}');
        return stringBuilder.toString();
    }

    /**
     * Identifier of Binlog, which is tablet-granularity
     */
    public static class BinlogIdVO implements Writable {

        @SerializedName("tabletId")
        long tabletId;

        public BinlogIdVO(long tabletId) {
            this.tabletId = tabletId;
        }

        public static BinlogIdVO read(DataInput input) throws IOException {
            return GsonUtils.GSON.fromJson(Text.readString(input), BinlogIdVO.class);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }

        public long getTabletId() {
            return tabletId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BinlogIdVO that = (BinlogIdVO) o;
            return tabletId == that.tabletId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tabletId);
        }

        @Override
        public String toString() {
            return "BinlogIdVO{" + "tabletId=" + tabletId + '}';
        }
    }

    /**
     * LSN of the binlog, which is identified by logical sequence and physical offset
     */
    public static class BinlogLSNVO implements Writable, Comparable<BinlogLSNVO> {
        @SerializedName("version")
        long version;

        @SerializedName("lsn")
        long lsn;

        public BinlogLSNVO(long version, long lsn) {
            this.version = version;
            this.lsn = lsn;
        }

        public TBinlogOffset toThrift() {
            TBinlogOffset res = new TBinlogOffset();
            res.setLsn(lsn);
            res.setVersion(version);
            return res;
        }

        public static BinlogLSNVO read(DataInput input) throws IOException {
            String json = Text.readString(input);
            return GsonUtils.GSON.fromJson(json, BinlogLSNVO.class);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }

        @Override
        public int compareTo(@NotNull BinlogLSNVO o) {
            return Long.compare(this.lsn, o.lsn);
        }

        public long getVersion() {
            return version;
        }

        public long getLsn() {
            return lsn;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BinlogLSNVO that = (BinlogLSNVO) o;
            return version == that.version && lsn == that.lsn;
        }

        @Override
        public int hashCode() {
            return Objects.hash(version, lsn);
        }

        @Override
        public String toString() {
            return "BinlogLSNVO{" +
                    "version=" + version +
                    "lsn=" + lsn +
                    '}';
        }
    }
}
