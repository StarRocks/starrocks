// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/HbPackage.java

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

package com.starrocks.persist;

import com.google.common.collect.Lists;
import com.starrocks.common.io.Writable;
import com.starrocks.system.HeartbeatResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class HbPackage implements Writable {

    private List<HeartbeatResponse> hbResults = Lists.newArrayList();

    public HbPackage() {

    }

    public void addHbResponse(HeartbeatResponse result) {
        hbResults.add(result);
    }

    public List<HeartbeatResponse> getHbResults() {
        return hbResults;
    }

    public static HbPackage read(DataInput in) throws IOException {
        HbPackage hbPackage = new HbPackage();
        hbPackage.readFields(in);
        return hbPackage;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(hbResults.size());
        for (HeartbeatResponse heartbeatResult : hbResults) {
            heartbeatResult.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            HeartbeatResponse result = HeartbeatResponse.read(in);
            hbResults.add(result);
        }
    }

}
