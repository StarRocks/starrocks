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
package com.starrocks.sql.common;

import com.starrocks.common.CdcErrorUtils;
import com.starrocks.common.ClassifiedRefreshFailure;
import com.starrocks.common.RefreshFailureKind;

/** The backend refused to read the changes an incremental refresh asked for. */
public class ChangeNotTrackableException extends DmlException implements ClassifiedRefreshFailure {
    private final transient CdcErrorUtils.Parsed rejection;

    public ChangeNotTrackableException(String message, CdcErrorUtils.Parsed rejection, Exception cause) {
        super(message);
        initCause(cause);
        this.rejection = rejection;
    }

    public CdcErrorUtils.Parsed getRejection() {
        return rejection;
    }

    @Override
    public RefreshFailureKind kind() {
        return RefreshFailureKind.INCREMENTAL_BROKEN;
    }
}
