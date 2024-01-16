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

package com.starrocks.backup.mv;

import com.starrocks.backup.BackupJobInfo;

public class MvBaseTableBackupInfo {
    private final BackupJobInfo.BackupTableInfo backupTableInfo;
    private final String localTableName;
    private final long remoteTableId;
    private final long backupTime;
    public MvBaseTableBackupInfo(BackupJobInfo.BackupTableInfo backupTableInfo,
                                 String localTableName,
                                 long remoteTableId,
                                 long backupTime) {
        this.backupTableInfo = backupTableInfo;
        this.localTableName = localTableName;
        this.remoteTableId = remoteTableId;
        this.backupTime = backupTime;
    }

    public BackupJobInfo.BackupTableInfo getBackupTableInfo() {
        return backupTableInfo;
    }

    public String getLocalTableName() {
        return localTableName;
    }

    public long getRemoteTableId() {
        return remoteTableId;
    }

    public long getBackupTime() {
        return backupTime;
    }
}
