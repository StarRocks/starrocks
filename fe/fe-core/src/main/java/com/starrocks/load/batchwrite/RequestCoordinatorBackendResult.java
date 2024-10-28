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

import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatus;

import java.util.List;

/**
 * Represents the result of a request for coordinator backends.
 * The value is a list of compute nodes that can accept write.
 */
public class RequestCoordinatorBackendResult extends StatusOr<List<ComputeNode>> {

    public RequestCoordinatorBackendResult(TStatus status, List<ComputeNode> result) {
        super(status, result);
    }
}
