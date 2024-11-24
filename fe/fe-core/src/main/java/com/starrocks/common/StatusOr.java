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

package com.starrocks.common;

import com.google.common.base.Preconditions;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;

/**
 * Abstract class representing a status or a value.
 * The value is only present if the status is OK.
 *
 * @param <T> The type of the value.
 */
public abstract class StatusOr<T> {

    protected final TStatus status;
    protected final T value;

    protected StatusOr(TStatus status, T value) {
        Preconditions.checkArgument((
                status.status_code == TStatusCode.OK && value != null)
                || (status.status_code != TStatusCode.OK && value == null));
        this.status = status;
        this.value = value;
    }

    public boolean isOk() {
        return status.status_code.equals(TStatusCode.OK);
    }

    public TStatus getStatus() {
        return status;
    }

    public T getValue() {
        return value;
    }
}
