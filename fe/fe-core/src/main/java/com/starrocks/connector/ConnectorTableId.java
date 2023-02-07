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

package com.starrocks.connector;

import com.starrocks.common.Id;
import com.starrocks.common.IdGenerator;

public class ConnectorTableId extends Id<ConnectorTableId> {
    public ConnectorTableId(int id) {
        super(id);
    }

    public static IdGenerator<ConnectorTableId> createGenerator() {
        return new IdGenerator<ConnectorTableId>() {
            @Override
            public synchronized ConnectorTableId getNextId() {
                return new ConnectorTableId(nextId++);
            }

            @Override
            public synchronized ConnectorTableId getMaxId() {
                return new ConnectorTableId(nextId - 1);
            }
        };
    }

    @Override
    public String toString() {
        return String.format("F%02d", id);
    }
}