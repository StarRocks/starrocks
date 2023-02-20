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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/LoadErrorHub.java

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

package com.starrocks.load;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Deprecated
public abstract class LoadErrorHub {
    private static final Logger LOG = LogManager.getLogger(LoadErrorHub.class);

    public static enum HubType {
        MYSQL_TYPE,
        BROKER_TYPE,
        NULL_TYPE
    }

    public class ErrorMsg {
        private long jobId;
        private String msg;

        public ErrorMsg(long id, String message) {
            jobId = id;
            msg = message;
        }

        public long getJobId() {
            return jobId;
        }

        public String getMsg() {
            return msg;
        }
    }

    public static class Param implements Writable {
        private HubType type;
        private MysqlLoadErrorHub.MysqlParam mysqlParam;
        private BrokerLoadErrorHub.BrokerParam brokerParam;

        // for replay
        public Param() {
            type = HubType.NULL_TYPE;
        }

        public static Param createMysqlParam(MysqlLoadErrorHub.MysqlParam mysqlParam) {
            Param param = new Param();
            param.type = HubType.MYSQL_TYPE;
            param.mysqlParam = mysqlParam;
            return param;
        }

        public static Param createBrokerParam(BrokerLoadErrorHub.BrokerParam brokerParam) {
            Param param = new Param();
            param.type = HubType.BROKER_TYPE;
            param.brokerParam = brokerParam;
            return param;
        }

        public static Param createNullParam() {
            Param param = new Param();
            param.type = HubType.NULL_TYPE;
            return param;
        }

        public HubType getType() {
            return type;
        }

        public String toString() {
            ToStringHelper helper = MoreObjects.toStringHelper(this);
            helper.add("type", type.toString());
            switch (type) {
                case MYSQL_TYPE:
                    helper.add("mysql_info", mysqlParam.toString());
                    break;
                case NULL_TYPE:
                    helper.add("mysql_info", "null");
                    break;
                default:
                    Preconditions.checkState(false, "unknown hub type");
            }

            return helper.toString();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, type.name());
            switch (type) {
                case MYSQL_TYPE:
                    mysqlParam.write(out);
                    break;
                case BROKER_TYPE:
                    brokerParam.write(out);
                    break;
                case NULL_TYPE:
                    break;
                default:
                    Preconditions.checkState(false, "unknown hub type");
            }
        }

        public void readFields(DataInput in) throws IOException {
            type = HubType.valueOf(Text.readString(in));
            switch (type) {
                case MYSQL_TYPE:
                    mysqlParam = new MysqlLoadErrorHub.MysqlParam();
                    mysqlParam.readFields(in);
                    break;
                case BROKER_TYPE:
                    brokerParam = new BrokerLoadErrorHub.BrokerParam();
                    brokerParam.readFields(in);
                    break;
                case NULL_TYPE:
                    break;
                default:
                    Preconditions.checkState(false, "unknown hub type");
            }
        }
    }

    public abstract boolean prepare();

    public abstract boolean close();
}
