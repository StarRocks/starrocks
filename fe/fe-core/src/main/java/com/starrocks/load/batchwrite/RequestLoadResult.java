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

package com.starrocks.load.batchwrite;

import com.starrocks.common.StatusOr;
import com.starrocks.thrift.TStatus;

/**
 * Represents the result of a load request. The value is a label
 * that can be used to track the load.
 */
public class RequestLoadResult extends StatusOr<String> {

    public RequestLoadResult(TStatus status, String label) {
        super(status, label);
    }
}
