// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ExprId.java

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

package com.starrocks.analysis;

import com.starrocks.common.Id;
import com.starrocks.common.IdGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExprId extends Id<ExprId> {
    private static final Logger LOG = LogManager.getLogger(ExprId.class);

    // Construction only allowed via an IdGenerator.
    public ExprId(int id) {
        super(id);
    }

    public static IdGenerator<ExprId> createGenerator() {
        return new IdGenerator<ExprId>() {
            @Override
            public ExprId getNextId() {
                return new ExprId(nextId++);
            }

            @Override
            public ExprId getMaxId() {
                return new ExprId(nextId - 1);
            }
        };
    }
}
