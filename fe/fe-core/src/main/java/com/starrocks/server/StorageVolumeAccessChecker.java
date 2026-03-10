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

package com.starrocks.server;

import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.HdfsUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class StorageVolumeAccessChecker {
    private StorageVolumeAccessChecker() {
    }

    public static void check(String svName, String svType, List<String> locations, Map<String, String> params)
            throws DdlException {
        for (String location : locations) {
            try {
                // checkPathExist() will trigger a real access attempt to validate credential/network/path reachability.
                HdfsUtil.checkPathExist(location, new HashMap<>(params));
            } catch (StarRocksException e) {
                Throwable cause = e;
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                String message = cause.getMessage();
                throw new DdlException(String.format(
                        "Storage volume accessibility check failed. storage volume: '%s', type: '%s', location: '%s', error: %s",
                        svName, svType, location, message == null ? cause.toString() : message));
            }
        }
    }
}
