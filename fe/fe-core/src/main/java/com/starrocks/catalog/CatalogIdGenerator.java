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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/CatalogIdGenerator.java

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

package com.starrocks.catalog;

import com.starrocks.server.GlobalStateMgr;

// This new Id generator is just same as TransactionIdGenerator.
// But we can't just use TransactionIdGenerator to replace the old globalStateMgr's 'nextId' for compatibility reason.
// cause they are using different edit log operation type.
public class CatalogIdGenerator {
    private static final int BATCH_ID_INTERVAL = 1000;

    private long nextId;
    private long batchEndId;

    public CatalogIdGenerator(long initValue) {
        nextId = initValue + 1;
        batchEndId = initValue;
    }

    // performance is more quickly
    public synchronized long getNextId() {
        if (nextId < batchEndId) {
            return nextId++;
        } else {
            batchEndId = batchEndId + BATCH_ID_INTERVAL;
            GlobalStateMgr.getCurrentState().getEditLog().logSaveNextId(batchEndId);
            return nextId++;
        }
    }

    public synchronized void setId(long id) {
        if (id > batchEndId) {
            batchEndId = id;
            nextId = id;
        }
    }

    // just for checkpoint, so no need to synchronize
    public long getBatchEndId() {
        return batchEndId;
    }
}
