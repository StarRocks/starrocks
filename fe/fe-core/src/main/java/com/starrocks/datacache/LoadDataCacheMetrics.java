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
import com.starrocks.monitor.unit.TimeValue;
import com.starrocks.thrift.TLoadDataCacheMetrics;

import java.util.concurrent.TimeUnit;

public class LoadDataCacheMetrics {
    private final ByteSizeValue readBytes;
    private final TimeValue readTimeNs;
    private final ByteSizeValue writeBytes;
    private final TimeValue writeTimeNs;

    // The number of metrics merged
    private final long count;

    private final DataCacheMetrics lastDataCacheMetrics;

    private LoadDataCacheMetrics(ByteSizeValue readBytes, TimeValue readTimeNs, ByteSizeValue writeBytes,
                                 TimeValue writeTimeNs, long count,
                                 DataCacheMetrics lastDataCacheMetrics) {
        this.readBytes = readBytes;
        this.readTimeNs = readTimeNs;
        this.writeBytes = writeBytes;
        this.writeTimeNs = writeTimeNs;
        this.count = count;
        this.lastDataCacheMetrics = lastDataCacheMetrics;
    }

    public static LoadDataCacheMetrics mergeMetrics(LoadDataCacheMetrics before, LoadDataCacheMetrics now) {
        ByteSizeValue mergedReadBytes =
                new ByteSizeValue(before.getReadBytes().getBytes() + now.getReadBytes().getBytes());
        TimeValue mergedReadTimeNs = new TimeValue(before.getReadTimeNs().nanos() + now.getReadTimeNs().nanos(),
                TimeUnit.NANOSECONDS);
        ByteSizeValue mergedWriteBytes =
                new ByteSizeValue(before.getWriteBytes().getBytes() + now.getWriteBytes().getBytes());
        TimeValue mergedWriteTimeNs =
                new TimeValue(before.getWriteTimeNs().nanos() + now.getWriteTimeNs().nanos(), TimeUnit.NANOSECONDS);
        long mergedCount = before.getCount() + now.getCount();
        return new LoadDataCacheMetrics(mergedReadBytes, mergedReadTimeNs, mergedWriteBytes, mergedWriteTimeNs,
                mergedCount, now.getLastDataCacheMetrics());
    }

    public static LoadDataCacheMetrics buildFromThrift(TLoadDataCacheMetrics tLoadDataCacheMetrics) {
        long readBytes = 0;
        long readTimeNs = 0;
        long writeBytes = 0;
        long writeTimeNs = 0;
        long count = 0;
        DataCacheMetrics dataCacheMetrics;
        if (tLoadDataCacheMetrics.isSetRead_bytes()) {
            readBytes = tLoadDataCacheMetrics.read_bytes;
        }
        if (tLoadDataCacheMetrics.isSetRead_time_ns()) {
            readTimeNs = tLoadDataCacheMetrics.read_time_ns;
        }
        if (tLoadDataCacheMetrics.isSetWrite_bytes()) {
            writeBytes = tLoadDataCacheMetrics.write_bytes;
        }
        if (tLoadDataCacheMetrics.isSetWrite_time_ns()) {
            writeTimeNs = tLoadDataCacheMetrics.write_time_ns;
        }
        if (tLoadDataCacheMetrics.isSetCount()) {
            count = tLoadDataCacheMetrics.count;
        }

        if (tLoadDataCacheMetrics.isSetMetrics()) {
            dataCacheMetrics = DataCacheMetrics.buildFromThrift(tLoadDataCacheMetrics.metrics);
        } else {
            dataCacheMetrics = DataCacheMetrics.buildEmpty();
        }

        return new LoadDataCacheMetrics(new ByteSizeValue(readBytes), new TimeValue(readTimeNs, TimeUnit.NANOSECONDS),
                new ByteSizeValue(writeBytes), new TimeValue(writeTimeNs, TimeUnit.NANOSECONDS), count, dataCacheMetrics);
    }

    public ByteSizeValue getReadBytes() {
        return readBytes;
    }

    public TimeValue getReadTimeNs() {
        return readTimeNs;
    }

    public ByteSizeValue getWriteBytes() {
        return writeBytes;
    }

    public TimeValue getWriteTimeNs() {
        return writeTimeNs;
    }

    public long getCount() {
        return count;
    }

    public DataCacheMetrics getLastDataCacheMetrics() {
        return lastDataCacheMetrics;
    }
}
