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
package com.starrocks.sql.analyzer.mv;

import com.google.common.base.Preconditions;
import com.starrocks.common.ClassifiedRefreshFailure;
import com.starrocks.common.RefreshFailureKind;
import com.starrocks.sql.analyzer.SemanticException;

public class IvmBrokenException extends SemanticException implements ClassifiedRefreshFailure {
    private final RefreshFailureKind kind;

    public IvmBrokenException(RefreshFailureKind kind, String detailMsg) {
        super(detailMsg);
        this.kind = checkBreaking(kind);
    }

    public IvmBrokenException(RefreshFailureKind kind, String detailMsg, Exception cause) {
        super(detailMsg, cause);
        this.kind = checkBreaking(kind);
    }

    private static RefreshFailureKind checkBreaking(RefreshFailureKind kind) {
        Preconditions.checkArgument(kind.isBreaking(), "%s does not break an incremental view", kind);
        return kind;
    }

    @Override
    public RefreshFailureKind kind() {
        return kind;
    }
}
