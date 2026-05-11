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

package com.starrocks.sql.optimizer.rewrite;

/**
 * Thrown by {@link ScalarOperatorTypeReDeriver} when a node's type or function
 * cannot be re-derived after a leaf type change. Caught and translated to
 * "reject this MV rewrite candidate" by MvColumnRefSubstitutor.
 *
 * <p>Note: ScalarOperatorTypeReDeriver and MvColumnRefSubstitutor are added in
 * subsequent phases; this exception is introduced first to avoid forward
 * references during incremental implementation.
 */
public class TypeReDeriveException extends RuntimeException {

    public TypeReDeriveException(String message) {
        super(message);
    }

    public TypeReDeriveException(String message, Throwable cause) {
        super(message, cause);
    }
}
