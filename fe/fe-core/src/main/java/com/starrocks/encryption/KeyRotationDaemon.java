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
package com.starrocks.encryption;

import com.starrocks.common.util.FrontendDaemon;

public class KeyRotationDaemon extends FrontendDaemon {
    private static final int KEY_ROTATION_CHECK_INTERVAL_MS = 10000;
    private final KeyMgr keyMgr;

    public KeyRotationDaemon(KeyMgr keyMgr) {
        super("key-rotation-daemon", KEY_ROTATION_CHECK_INTERVAL_MS);
        this.keyMgr = keyMgr;
    }

    @Override
    protected void runAfterCatalogReady() {
        keyMgr.checkKeyRotation();
    }
}
