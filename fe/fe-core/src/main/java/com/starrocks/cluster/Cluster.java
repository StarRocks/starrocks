// This file is made available under Elastic License 2.0.
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * cluster only save db and user's id and name
 */
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

    public String getName() {
        return name;
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

        // For compatible for backendIdSet
        out.writeLong(0);

        // dbNames and dbIds are not used anymore, so we write two zeros here.
        out.writeInt(0);
        out.writeInt(0);

        // For back compatible, write two zeros for linkDbNames and linkDbIds
        out.writeInt(0);
        out.writeInt(0);
    }

    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        name = Text.readString(in);

        // For compatible for backendIdSet: List<Long>
        Long len = in.readLong();
        while (len-- > 0) {
            in.readLong();
        }
        // compatible for dbNames, skip a string
        int count = in.readInt();
        while (count-- > 0) {
            Text.readString(in);
        }
        // compatible for dbIds, skip a long
        count = in.readInt();
        while (count-- > 0) {
            in.readLong();
        }

        // For back compatible, write two zeros for linkDbNames and linkDbIds
        count = in.readInt();
        if (count > 0) {
            throw new IOException("linkDbNames in Cluster should be equal with 0, now is " + count);
        }
        count = in.readInt();
        if (count > 0) {
            throw new IOException("linkDbIds in Cluster should be equal with 0, now is " + count);
        }
    }
}
