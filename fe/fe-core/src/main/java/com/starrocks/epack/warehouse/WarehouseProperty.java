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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/OlapTable.java

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

package com.starrocks.epack.warehouse;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WarehouseProperty {
    private static final Logger LOG = LogManager.getLogger(WarehouseProperty.class);

    public static final String PROPERTY_COMPUTE_REPLICA = "compute_replica";

    public static final int DEFAULT_REPLICA_NUMBER = 1;

    @SerializedName(value = "compute_replica")
    private int computeReplica = DEFAULT_REPLICA_NUMBER;

    public WarehouseProperty() {
        this.computeReplica = DEFAULT_REPLICA_NUMBER;
    }

    public void setComputeReplica(int computeReplica) {
        this.computeReplica = computeReplica;
    }

    public int getComputeReplica() {
        return computeReplica;
    }

    public String toString() {
        return new Gson().toJson(this);
    }
}
