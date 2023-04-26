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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/BrokerLoadErrorHub.java

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

import com.google.common.collect.Maps;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TBrokerErrorHubInfo;
import com.starrocks.thrift.TNetworkAddress;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class BrokerLoadErrorHub extends LoadErrorHub {

    private BrokerParam brokerParam;

    public BrokerLoadErrorHub(BrokerParam brokerParam) {
        this.brokerParam = brokerParam;
    }

    public BrokerParam getBrokerParam() {
        return brokerParam;
    }

    public static class BrokerParam implements Writable {
        private String brokerName;
        private String path;
        private Map<String, String> prop = Maps.newHashMap();

        // for persist
        public BrokerParam() {
        }

        public BrokerParam(String brokerName, String path, Map<String, String> prop) {
            this.brokerName = brokerName;
            this.path = path;
            this.prop = prop;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, brokerName);
            Text.writeString(out, path);
            out.writeInt(prop.size());
            for (Map.Entry<String, String> entry : prop.entrySet()) {
                Text.writeString(out, entry.getKey());
                Text.writeString(out, entry.getValue());
            }
        }

        public void readFields(DataInput in) throws IOException {
            brokerName = Text.readString(in);
            path = Text.readString(in);
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = Text.readString(in);
                String val = Text.readString(in);
                prop.put(key, val);
            }
        }

        public TBrokerErrorHubInfo toThrift() {
            FsBroker fsBroker = GlobalStateMgr.getCurrentState().getBrokerMgr().getAnyBroker(brokerName);
            if (fsBroker == null) {
                return null;
            }
            TBrokerErrorHubInfo info = new TBrokerErrorHubInfo(new TNetworkAddress(fsBroker.ip, fsBroker.port),
                    path, prop);
            return info;
        }

        public String getBrief() {
            Map<String, String> briefMap = Maps.newHashMap(prop);
            briefMap.put("name", brokerName);
            briefMap.put("path", path);
            PrintableMap<String, String> printableMap = new PrintableMap<>(briefMap, "=", true, false, true);
            return printableMap.toString();
        }
    }

    @Override
    public boolean prepare() {
        return true;
    }

    @Override
    public boolean close() {
        return true;
    }

}
