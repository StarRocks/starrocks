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

package com.starrocks.http.rest.transaction;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;

import java.util.List;

/**
 * Request params parser, validator and holder.
 */
public class TransactionOperationParams {

    /* headers */

    private final String dbName;
    private final String tableName;
    private final String warehouseName;
    private final String label;
    private final TransactionOperation txnOperation;
    private final Long timeoutMillis;
    private final Channel channel;

    /* queries */

    private final LoadJobSourceType sourceType;

    /* body */

    private final Body body;

    public TransactionOperationParams(String dbName,
                                      String tableName,
                                      String warehouseName,
                                      String label,
                                      TransactionOperation txnOperation,
                                      Long timeoutMillis,
                                      Channel channel,
                                      LoadJobSourceType sourceType,
                                      Body body) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.warehouseName = warehouseName;
        this.label = label;
        this.txnOperation = txnOperation;
        this.timeoutMillis = timeoutMillis;
        this.channel = channel;
        this.sourceType = sourceType;
        this.body = body;
    }

    /**
     * Channel info (eg. id, num) in request.
     */
    public static class Channel {

        private final Integer id;
        private final Integer num;

        public Channel(Integer id, Integer num) {
            this.id = id;
            this.num = num;
        }

        public boolean isNull() {
            return !notNull();
        }

        public boolean notNull() {
            return null != id && null != num;
        }

        public Integer getId() {
            return id;
        }

        public Integer getNum() {
            return num;
        }
    }

    /**
     * Request body in JSON format.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Body {

        @JsonProperty("committed_tablets")
        private List<TabletCommitInfo> committedTablets;

        @JsonProperty("failed_tablets")
        private List<TabletFailInfo> failedTablets;

        public Body() {
        }

        public List<TabletCommitInfo> getCommittedTablets() {
            return committedTablets;
        }

        public void setCommittedTablets(List<TabletCommitInfo> committedTablets) {
            this.committedTablets = committedTablets;
        }

        public List<TabletFailInfo> getFailedTablets() {
            return failedTablets;
        }

        public void setFailedTablets(List<TabletFailInfo> failedTablets) {
            this.failedTablets = failedTablets;
        }
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getLabel() {
        return label;
    }

    public String getWarehouseName() {
        return warehouseName;
    }

    public TransactionOperation getTxnOperation() {
        return txnOperation;
    }

    public Long getTimeoutMillis() {
        return timeoutMillis;
    }

    public Channel getChannel() {
        return channel;
    }

    public LoadJobSourceType getSourceType() {
        return sourceType;
    }

    public Body getBody() {
        return body;
    }
}