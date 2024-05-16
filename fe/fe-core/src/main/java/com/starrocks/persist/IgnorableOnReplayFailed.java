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

package com.starrocks.persist;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Add this annotation for OperationTypes whose failure will not damage cluster data.
 * For example OP_CREATE_TABLE_V2 cannot be annotated as IgnorableOnReplayFailed,
 * because if the table creation fails, the table data will be lost.
 * But OP_ADD_ANALYZER_JOB can be annotated as IgnorableOnReplayFailed,
 * because the loss of the analyze job will not affect the cluster data, and the analyze job can be easily recreated
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface IgnorableOnReplayFailed {
}
