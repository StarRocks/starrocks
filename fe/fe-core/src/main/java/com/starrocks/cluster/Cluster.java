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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/cluster/Cluster.java

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

package com.starrocks.cluster;

import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.LinkDbInfo;
import com.starrocks.system.SystemInfoService;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Now Cluster don't have read interface, in order to be back compatible.
// We will remove the persistent format later.
@Deprecated
public class Cluster implements Writable {
    private Long id;
    private String name;

    private Cluster() {
        // for persist
    }

    public Cluster(String name, long id) {
        this.name = name;
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public boolean isDefaultCluster() {
        return SystemInfoService.DEFAULT_CLUSTER.equalsIgnoreCase(name);
    }

    public static Cluster read(DataInput in) throws IOException {
        Cluster cluster = new Cluster();
        cluster.readFields(in);
        return cluster;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        Text.writeString(out, name);

        // backendIdSet: Set<Long>
        out.writeLong(0);

        // dbNames: Set<String>
        out.writeInt(0);

        // dbIds: Set<Long>
        out.writeInt(0);

        // linkDbNames: ConcurrentHashMap<String, LinkDbInfo>
        out.writeInt(0);

        // linkDbIds: ConcurrentHashMap<Long, LinkDbInfo>
        out.writeInt(0);
    }

    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        name = Text.readString(in);

        // backendIdSet: Set<Long>
        long len = in.readLong();
        while (len-- > 0) {
            in.readLong();
        }

        // dbNames: Set<String>
        int count = in.readInt();
        while (count-- > 0) {
            Text.readString(in);
        }

        // dbIds: Set<Long>
        count = in.readInt();
        while (count-- > 0) {
            in.readLong();
        }

        // linkDbNames: ConcurrentHashMap<String, LinkDbInfo>
        count = in.readInt();
        while (count-- > 0) {
            Text.readString(in);
            final LinkDbInfo value = new LinkDbInfo();
            value.readFields(in);
        }

        // linkDbIds: ConcurrentHashMap<Long, LinkDbInfo>
        count = in.readInt();
        while (count-- > 0) {
            in.readLong();
            final LinkDbInfo value = new LinkDbInfo();
            value.readFields(in);
        }
    }
}
