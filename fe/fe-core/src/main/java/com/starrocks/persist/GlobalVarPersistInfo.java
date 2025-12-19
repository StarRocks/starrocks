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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/GlobalVarPersistInfo.java

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

import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// for persist global variables
public class GlobalVarPersistInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(GlobalVarPersistInfo.class);
    private String varName;

    private Object val;

    // the modified variable info will be saved as a json string
    private String persistJsonString;

    public GlobalVarPersistInfo() {
        // for persist
    }

    public GlobalVarPersistInfo(String varName, Object val) {
        this.varName = varName;
        this.val = val;
    }

    public void setPersistJsonString(String persistJsonString) {
        this.persistJsonString = persistJsonString;
    }

    public String getPersistJsonString() {
        return persistJsonString;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        try {
            JSONObject root = new JSONObject();
            root.put(varName, val);
            Text.writeString(out, root.toString());
        } catch (Exception e) {
            throw new IOException("failed to write session variable: " + e.getMessage());
        }
    }

    public static GlobalVarPersistInfo read(DataInput in) throws IOException {
        GlobalVarPersistInfo info = new GlobalVarPersistInfo();
        info.persistJsonString = Text.readString(in);
        return info;
    }
}
