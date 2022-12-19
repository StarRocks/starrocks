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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/UserResource.java

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

package com.starrocks.mysql.privilege;

import com.starrocks.common.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Deprecated
// Keep it for serialization compatibility
// Resource belong to one user
public class UserResource {

    public static void readIn(DataInput in) throws IOException {
        // Useless code, but to keep backward compatible
        int numResource = in.readInt();
        for (int i = 0; i < numResource; ++i) {
            int code = in.readInt();
            int value = in.readInt();
        }

        int numGroup = in.readInt();
        for (int i = 0; i < numGroup; ++i) {
            Text.readString(in);
            in.readInt();
        }
    }

    public static void write(DataOutput out) throws IOException {
        // Num resources
        out.writeInt(0);
        // Num groups
        out.writeInt(0);
    }
}
