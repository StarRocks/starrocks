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

package com.starrocks.common.util;

/**
 * What a credential (password, secret key, token, ...) is printed as wherever the FE shows or logs one.
 */
public final class CredentialMask {
    /**
     * The mask to use. The BE masks with the same value (kCredentialMask in be/src/base/auth/credential_mask.h).
     */
    public static final String LONG = "******";

    /**
     * Only for the outputs that already printed "***" (SHOW CREATE through PrintableMap, SQL redacted by
     * SqlCredentialRedactor, the UDF location), kept so they do not change. Do not use it in new code.
     */
    public static final String SHORT = "***";

    private CredentialMask() {
    }
}
