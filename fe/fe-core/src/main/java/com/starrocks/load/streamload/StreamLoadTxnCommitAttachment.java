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


package com.starrocks.load.streamload;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StreamLoadTxnCommitAttachment extends TxnCommitAttachment implements Writable {
    @SerializedName(value = "trackingURL")
    private String trackingURL;
    @SerializedName(value = "beforeLoadTimeMs")
    private long beforeLoadTimeMs;
    @SerializedName(value = "startLoadingTimeMs")
    private long startLoadingTimeMs;
    @SerializedName(value = "startPreparingTimeMs")
    private long startPreparingTimeMs;
    @SerializedName(value = "finishPreparingTimeMs")
    private long finishPreparingTimeMs;
    @SerializedName(value = "endTimeMs")
    private long endTimeMs;
    @SerializedName(value = "numRowsNormal")
    private long numRowsNormal;
    @SerializedName(value = "numRowsAbnormal")
    private long numRowsAbnormal;
    @SerializedName(value = "numRowsUnselected")
    private long numRowsUnselected;
    @SerializedName(value = "numLoadBytesTotal")
    private long numLoadBytesTotal;

    public StreamLoadTxnCommitAttachment() {
        super(TransactionState.LoadJobSourceType.FRONTEND_STREAMING);
    }

    public StreamLoadTxnCommitAttachment(
            long beforeLoadTimeMs, long startLoadingTimeMs,
            long startPreparingTimeMs, long finishPreparingTimeMs,
            long endTimeMs, long numRowsNormal, long numRowsAbnormal,
            long numRowsUnselected, long numLoadBytesTotal,
            String trackingURL) {
        super(TransactionState.LoadJobSourceType.FRONTEND_STREAMING);
        this.trackingURL = trackingURL;
        this.beforeLoadTimeMs = beforeLoadTimeMs;
        this.startLoadingTimeMs = startLoadingTimeMs;
        this.startPreparingTimeMs = startPreparingTimeMs;
        this.finishPreparingTimeMs = finishPreparingTimeMs;
        this.endTimeMs = endTimeMs;
        this.numRowsNormal = numRowsNormal;
        this.numRowsAbnormal = numRowsAbnormal;
        this.numRowsUnselected = numRowsUnselected;
        this.numLoadBytesTotal = numLoadBytesTotal;
    }

    public String getTrackingURL() {
        return trackingURL;
    }

    public long getBeforeLoadTimeMs() {
        return beforeLoadTimeMs;
    }

    public long getStartLoadingTimeMs() {
        return startLoadingTimeMs;
    }

    public long getStartPreparingTimeMs() {
        return startPreparingTimeMs;
    }

    public long getFinishPreparingTimeMs() {
        return finishPreparingTimeMs;
    }

    public long getEndTimeMs() {
        return endTimeMs;
    }

    public long getNumRowsNormal() {
        return numRowsNormal;
    }

    public long getNumRowsAbnormal() {
        return numRowsAbnormal;
    }

    public long getNumRowsUnselected() {
        return numRowsUnselected;
    }

    public long getNumLoadBytesTotal() {
        return numLoadBytesTotal;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public void readFields(DataInput in) throws IOException {}

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    public static StreamLoadTxnCommitAttachment loadStreamLoadTxnCommitAttachment(DataInput in) throws IOException {
        String json = Text.readString(in);
        StreamLoadTxnCommitAttachment attachment = GsonUtils.GSON.fromJson(json, StreamLoadTxnCommitAttachment.class);
        return attachment;
    }
}
