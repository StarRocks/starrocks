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

package com.starrocks.common.proc;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.storagevolume.StorageVolume;

public class StorageVolumeProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> STORAGE_VOLUME_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name")
            .add("Type")
            .add("IsDefault")
            .add("Location")
            .add("Params")
            .add("Enabled")
            .add("Comment")
            .build();

    private String storageVolumeName;

    public StorageVolumeProcNode(String storageVolumeName) {
        this.storageVolumeName = storageVolumeName;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(STORAGE_VOLUME_PROC_NODE_TITLE_NAMES);
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        StorageVolumeMgr storageVolumeMgr = globalStateMgr.getStorageVolumeMgr();
        StorageVolume sv = storageVolumeMgr.getStorageVolumeByName(storageVolumeName);
        if (sv == null) {
            return result;
        }
        sv.getProcNodeData(result);
        return result;
    }
}
