// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import lombok.Value;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Binlog group consumption state of MV
 * NOTE: not thread-safe for updating
 */
@Value
public class BinlogConsumeStateVO implements Writable {
    Map<BinlogIdVO, BinlogLSNVO> binlogMap = new HashMap<>();

    public static BinlogConsumeStateVO read(DataInput input) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(input), BinlogConsumeStateVO.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    /**
     * Identifier of Binlog, which is tablet-granularity
     */
    @Value
    public static class BinlogIdVO implements Writable {
        @SerializedName("dbId")
        long dbId;

        @SerializedName("tableId")
        long tableId;

        @SerializedName("tabletId")
        long tabletId;

        public static BinlogIdVO read(DataInput input) throws IOException {
            return GsonUtils.GSON.fromJson(Text.readString(input), BinlogIdVO.class);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }
    }

    /**
     * LSN of the binlog, which is identified by logical sequence and physical offset
     */
    @Value
    public static class BinlogLSNVO implements Writable, Comparable<BinlogLSNVO> {
        @SerializedName("version")
        long version;

        @SerializedName("sequence")
        long logicalSequence;

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
            return Long.compare(this.logicalSequence, o.logicalSequence);
        }
    }
}
