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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class StorageVolumeAccessChecker {
    private static final Logger LOG = LogManager.getLogger(StorageVolumeAccessChecker.class);

    private StorageVolumeAccessChecker() {
    }

    public static void check(String svName, String svType, List<String> locations, Map<String, String> params)
            throws DdlException {
        for (String location : locations) {
            // Write an empty temp file and then delete it to verify that the credentials have
            // actual write access to the location. checkPathExist() only verifies path existence,
            // not write permission, and will silently succeed when the path does not yet exist.
            String normalizedLoc = location.endsWith("/") ? location : location + "/";
            String tempPath = normalizedLoc + ".starrocks_sv_access_check_" + UUID.randomUUID();
            try {
                HdfsUtil.writeFile(new byte[0], tempPath, new HashMap<>(params));
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
            // Best-effort cleanup; a leftover temp file is harmless but we should try to remove it.
            try {
                HdfsUtil.deletePath(tempPath, new HashMap<>(params));
            } catch (StarRocksException e) {
                LOG.warn("StorageVolumeAccessChecker: failed to delete temp check file {}: {}", tempPath, e.getMessage());
            }
        }
    }
}
