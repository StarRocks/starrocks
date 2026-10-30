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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.common.TimeoutException;

/**
 * Internal signal raised by {@link PreSplitPipeline#preSubmit} when the
 * pre-submit phase (sample + plan + build job) does not finish within the
 * configured timeout. The coordinator catches this and maps it to
 * {@link SkipReason#TIMEOUT_PRE_SUBMIT}; the load proceeds against the
 * original single tablet.
 */
public final class PreSplitPreSubmitTimeoutException extends TimeoutException {
    private static final long serialVersionUID = 1L;

    public PreSplitPreSubmitTimeoutException(String reason) {
        super(reason);
    }
}
