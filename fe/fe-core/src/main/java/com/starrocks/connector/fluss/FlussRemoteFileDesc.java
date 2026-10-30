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

package com.starrocks.connector.fluss;

import com.starrocks.connector.RemoteFileDesc;

public class FlussRemoteFileDesc extends RemoteFileDesc {

    private FlussSplitsInfo flussSplitsInfo;

    private FlussRemoteFileDesc(FlussSplitsInfo flussSplitsInfo) {
        super(null, null, 0, 0, null);
        this.flussSplitsInfo = flussSplitsInfo;
    }

    public static FlussRemoteFileDesc createFlussRemoteFileDesc(FlussSplitsInfo flussSplitsInfo) {
        return new FlussRemoteFileDesc(flussSplitsInfo);
    }

    public FlussSplitsInfo getFlussSplitsInfo() {
        return flussSplitsInfo;
    }

    @Override
    public String toString() {
        return "FlussRemoteFileDesc{" +
                "flussSplitsInfo=" + flussSplitsInfo +
                '}';
    }
}
