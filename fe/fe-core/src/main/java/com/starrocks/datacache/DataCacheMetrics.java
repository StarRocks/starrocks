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

import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.thrift.TDataCacheMetrics;

public class DataCacheMetrics {
    public enum Status {

        DISABLED("Disabled"), NORMAL("Normal"), UPDATING("Updating"), LOADING("Loading"), ABNORMAL("Abnormal");

        private final String name;

        Status(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }

    private final Status status;
    private final ByteSizeValue memQuoteBytes;
    private final ByteSizeValue memUsedBytes;
    private final ByteSizeValue diskQuotaBytes;
    private final ByteSizeValue diskUsedBytes;

    private DataCacheMetrics(Status status, ByteSizeValue memQuoteBytes, ByteSizeValue memUsedBytes,
                             ByteSizeValue diskQuotaBytes,
                             ByteSizeValue diskUsedBytes) {
        this.status = status;
        this.memQuoteBytes = memQuoteBytes;
        this.memUsedBytes = memUsedBytes;
        this.diskQuotaBytes = diskQuotaBytes;
        this.diskUsedBytes = diskUsedBytes;
    }

    public static DataCacheMetrics buildEmpty() {
        return new DataCacheMetrics(Status.DISABLED, new ByteSizeValue(0), new ByteSizeValue(0), new ByteSizeValue(0),
                new ByteSizeValue(0));
    }

    public static DataCacheMetrics buildFromThrift(TDataCacheMetrics tMetrics) {
        Status status = null;
        if (tMetrics.isSetStatus()) {
            switch (tMetrics.status) {
                case NORMAL:
                    status = Status.NORMAL;
                    break;
                case UPDATING:
                    status = Status.UPDATING;
                    break;
                case ABNORMAL:
                    status = Status.ABNORMAL;
                    break;
                default:
                    status = Status.DISABLED;
            }
        }

        long memQuoteBytes = tMetrics.isSetMem_quota_bytes() ? tMetrics.mem_quota_bytes : 0;
        long memUsedBytes = tMetrics.isSetMem_used_bytes() ? tMetrics.mem_used_bytes : 0;
        long diskQuotaBytes = tMetrics.isSetDisk_quota_bytes() ? tMetrics.disk_quota_bytes : 0;
        long diskUsedBytes = tMetrics.isSetDisk_used_bytes() ? tMetrics.disk_used_bytes : 0;

        return new DataCacheMetrics(status, new ByteSizeValue(memQuoteBytes), new ByteSizeValue(memUsedBytes),
                new ByteSizeValue(diskQuotaBytes), new ByteSizeValue(diskUsedBytes));
    }

    public String getMemUsageStr() {
        return String.format("%s/%s", memUsedBytes, memQuoteBytes);
    }

    public String getDiskUsageStr() {
        return String.format("%s/%s", diskUsedBytes, diskQuotaBytes);
    }

    public double getCacheUsage() {
        return ((double) diskUsedBytes.getBytes() + memUsedBytes.getBytes()) /
                (diskQuotaBytes.getBytes() + memQuoteBytes.getBytes());
    }

    public Status getStatus() {
        return status;
    }

    public ByteSizeValue getMemQuoteBytes() {
        return memQuoteBytes;
    }

    public ByteSizeValue getMemUsedBytes() {
        return memUsedBytes;
    }

    public ByteSizeValue getDiskQuotaBytes() {
        return diskQuotaBytes;
    }

    public ByteSizeValue getDiskUsedBytes() {
        return diskUsedBytes;
    }

    @Override
    public String toString() {
        return String.format("Status: %s, DiskUsed: %s, MemUsed: %s, DiskQuota: %s, MemQuota: %s", getStatus(),
                getDiskUsedBytes(), getMemUsedBytes(), getDiskQuotaBytes(), getMemQuoteBytes());
    }
}
