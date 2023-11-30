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

package com.starrocks.datacache;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.starrocks.thrift.TDataCacheDiskDirSpace;
import com.starrocks.thrift.TDataCacheMetrics;

import java.util.ArrayList;
import java.util.List;

public class DataCacheMetrics {
    public enum Status {

        NORMAL("Normal"), UPDATING("Updating"), ABNORMAL("Abnormal");

        private final String name;

        private Status(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }

    static class DiskDirSpace {
        String path;
        long quotaBytes;

        public DiskDirSpace(String path, long quotaBytes) {
            this.path = path;
            this.quotaBytes = quotaBytes;
        }
    }

    private final Status status;
    private final long memQuoteBytes;
    private final long memUsedBytes;
    private final long diskQuotaBytes;
    private final long diskUsedBytes;
    private final long metaUsedBytes;
    private final List<DiskDirSpace> diskDirSpaces;

    private DataCacheMetrics(Status status, long memQuoteBytes, long memUsedBytes, long diskQuotaBytes,
                             long diskUsedBytes,
                             long metaUsedBytes, List<DiskDirSpace> diskDirSpaces) {
        this.status = status;
        this.memQuoteBytes = memQuoteBytes;
        this.memUsedBytes = memUsedBytes;
        this.diskQuotaBytes = diskQuotaBytes;
        this.diskUsedBytes = diskUsedBytes;
        this.metaUsedBytes = metaUsedBytes;
        Preconditions.checkNotNull(diskDirSpaces, "Don't use null list here");
        this.diskDirSpaces = diskDirSpaces;
    }

    public static DataCacheMetrics buildFromThrift(TDataCacheMetrics tMetrics) {
        Status status = Status.ABNORMAL;
        if (tMetrics.isSetStatus()) {
            switch (tMetrics.status) {
                case NORMAL:
                    status = Status.NORMAL;
                    break;
                case UPDATING:
                    status = Status.UPDATING;
                    break;
                default:
            }
        }

        long memQuoteBytes = tMetrics.isSetMem_quota_bytes() ? tMetrics.mem_quota_bytes : 0;
        long memUsedBytes = tMetrics.isSetMem_used_bytes() ? tMetrics.mem_used_bytes : 0;
        long diskQuotaBytes = tMetrics.isSetDisk_quota_bytes() ? tMetrics.disk_quota_bytes : 0;
        long diskUsedBytes = tMetrics.isSetDisk_used_bytes() ? tMetrics.disk_used_bytes : 0;
        long metaUsedBytes = tMetrics.isSetMeta_used_bytes() ? tMetrics.meta_used_bytes : 0;

        List<DiskDirSpace> diskDirSpaces = Lists.newArrayList();
        if (tMetrics.isSetDisk_dir_spaces()) {
            for (TDataCacheDiskDirSpace tDataCacheDiskDirSpace : tMetrics.disk_dir_spaces) {
                String path = tDataCacheDiskDirSpace.isSetPath() ? tDataCacheDiskDirSpace.path : "";
                long quotaBytes = tDataCacheDiskDirSpace.isSetQuota_bytes() ? tDataCacheDiskDirSpace.quota_bytes : 0;
                diskDirSpaces.add(new DiskDirSpace(path, quotaBytes));
            }
        }

        return new DataCacheMetrics(status, memQuoteBytes, memUsedBytes, diskQuotaBytes, diskUsedBytes, metaUsedBytes,
                diskDirSpaces);
    }

    public String getMemUsage() {
        return String.format("%s/%s", convertSize(memUsedBytes), convertSize(memQuoteBytes));
    }

    public String getDiskUsage() {
        return String.format("%s/%s", convertSize(diskUsedBytes), convertSize(diskQuotaBytes));
    }

    public String getMetaUsage() {
        return String.format("%s", convertSize(metaUsedBytes));
    }

    public String getDiskInfo() {
        List<String> spaces = new ArrayList<>(diskDirSpaces.size());
        for (DiskDirSpace space : diskDirSpaces) {
            spaces.add(String.format("[Path: %s, Quota: %s]", space.path, convertSize(space.quotaBytes)));
        }
        return String.join(", ", spaces);
    }

    public Status getStatus() {
        return status;
    }

    private static String convertSize(long size) {
        double base1Gigabytes = 1024 * 1024 * 1024;
        double res = size / base1Gigabytes;
        return String.format("%.2fGb", res);
    }
}
