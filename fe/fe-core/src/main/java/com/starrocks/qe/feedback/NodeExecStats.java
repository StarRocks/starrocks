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

package com.starrocks.qe.feedback;

import com.starrocks.proto.NodeExecStatsItemPB;

public class NodeExecStats {

    private final int nodeId;

    private final long pushRows;

    private final long pullRows;

    private final long predFilterRows;

    private final long indexFilterRows;

    private final long rfFilterRows;

    public static final NodeExecStats EMPTY = new NodeExecStats(-1, -1, -1, -1, -1, -1);

    public NodeExecStats(int nodeId, long pushRows, long pullRows, long predFilterRows, long indexFilterRows,
                         long rfFilterRows) {
        this.nodeId = nodeId;
        this.pushRows = pushRows;
        this.pullRows = pullRows;
        this.predFilterRows = predFilterRows;
        this.indexFilterRows = indexFilterRows;
        this.rfFilterRows = rfFilterRows;
    }

    public static NodeExecStats buildFromPB(NodeExecStatsItemPB itemPB) {
        return new NodeExecStats(itemPB.getNodeId(), itemPB.getPushRows(), itemPB.getPullRows(), itemPB.getPredFilterRows(),
                itemPB.getIndexFilterRows(), itemPB.getRfFilterRows());
    }

    public int getNodeId() {
        return nodeId;
    }

    public long getPushRows() {
        return pushRows;
    }

    public long getPullRows() {
        return pullRows;
    }

    public long getPredFilterRows() {
        return predFilterRows;
    }

    public long getIndexFilterRows() {
        return indexFilterRows;
    }

    public long getRfFilterRows() {
        return rfFilterRows;
    }

    public static final class Builder {
        private int nodeId;

        private long pushRows;

        private long pullRows;

        private long predFilterRows;

        private long indexFilterRows;

        private long rfFilterRows;

        public Builder() {

        }

        public Builder(int nodeId, long pushRows, long pullRows, long predFilterRows, long indexFilterRows, long rfFilterRows) {
            this.nodeId = nodeId;
            this.pushRows = pushRows;
            this.pullRows = pullRows;
            this.predFilterRows = predFilterRows;
            this.indexFilterRows = indexFilterRows;
            this.rfFilterRows = rfFilterRows;
        }

        public NodeExecStats build() {
            return new NodeExecStats(nodeId, pushRows, pullRows, predFilterRows, indexFilterRows, rfFilterRows);
        }

        public static Builder buildFrom(NodeExecStats execStats) {
            return new Builder(execStats.getNodeId(), execStats.getPullRows(), execStats.getPullRows(),
                    execStats.getPredFilterRows(), execStats.getIndexFilterRows(), execStats.getRfFilterRows());
        }

        public Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder setPushRows(long pushRows) {
            this.pushRows = pushRows;
            return this;
        }

        public Builder setPullRows(long pullRows) {
            this.pullRows = pullRows;
            return this;
        }

        public Builder setPredFilterRows(long predFilterRows) {
            this.predFilterRows = predFilterRows;
            return this;
        }

        public Builder setIndexFilterRows(long indexFilterRows) {
            this.indexFilterRows = indexFilterRows;
            return this;
        }

        public Builder setRfFilterRows(long rfFilterRows) {
            this.rfFilterRows = rfFilterRows;
            return this;
        }
    }
}
