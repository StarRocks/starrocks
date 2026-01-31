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

package com.starrocks.service.arrow.flight.sql;

import com.google.common.base.Preconditions;
import com.starrocks.thrift.TUniqueId;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.Nullable;

public class ArrowFlightSqlResultDescriptor {
    private final Schema schema;
    @Nullable
    private final BackendResultDescriptor backendResultDescriptor;

    public ArrowFlightSqlResultDescriptor(long backendId, TUniqueId fragmentInstanceId, Schema schema) {
        this.backendResultDescriptor = new BackendResultDescriptor(backendId, fragmentInstanceId);
        this.schema = schema;
    }

    public ArrowFlightSqlResultDescriptor(Schema schema) {
        this.backendResultDescriptor = null;
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }

    public boolean isBackendResultDescriptor() {
        return backendResultDescriptor != null;
    }

    public long getBackendId() {
        Preconditions.checkArgument(backendResultDescriptor != null, "backend result not found");
        return backendResultDescriptor.backendId();
    }

    public TUniqueId getFragmentInstanceId() {
        Preconditions.checkArgument(backendResultDescriptor != null, "backend result not found");
        return backendResultDescriptor.fragmentInstanceId();
    }

    public record BackendResultDescriptor(long backendId, TUniqueId fragmentInstanceId) {
    }
}
