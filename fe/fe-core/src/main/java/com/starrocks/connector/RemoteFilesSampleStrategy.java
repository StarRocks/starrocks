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

package com.starrocks.connector;

import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

public class RemoteFilesSampleStrategy {
    public enum Type {
        ALL,
        RANDOM,
        SPECIFIC,
    }
    private int limit = 5;
    private Type type = Type.ALL;
    private int sampled = 0;
    private String fileName;

    private Predicate<THdfsScanRange> checker = x -> true;

    public RemoteFilesSampleStrategy() {}

    public RemoteFilesSampleStrategy(int limit, Predicate<THdfsScanRange> checker) {
        this.limit = limit;
        this.type = Type.RANDOM;
        this.checker = checker;
    }

    public RemoteFilesSampleStrategy(String fileName) {
        this.limit = 1;
        this.type = Type.SPECIFIC;
        this.fileName = fileName;
    }

    public int getLimit() {
        return limit;
    }

    public Type getType() {
        return type;
    }

    public boolean isSample() {
        return type != Type.ALL;
    }

    public boolean enough() {
        return type != Type.ALL && sampled >= limit;
    }

    private boolean supportedFormat(List<TScanRangeLocations> input) {
        return !input.isEmpty() &&
                input.get(0).getScan_range().isSetHdfs_scan_range() &&
                checker.test(input.get(0).getScan_range().getHdfs_scan_range());
    }

    public List<TScanRangeLocations> sample(List<TScanRangeLocations> input) {
        switch (type) {
            case ALL -> {
                return input;
            }
            case RANDOM -> {
                if (!supportedFormat(input)) {
                    sampled = limit;
                    return List.of();
                }
                int s = Math.max(0, Math.min(input.size(), limit - sampled));
                sampled += s;
                Collections.shuffle(input);
                return input.subList(0, s);
            }
            case SPECIFIC -> {
                for (TScanRangeLocations scan : input) {
                    if (scan.isSetScan_range() && scan.getScan_range().isSetHdfs_scan_range()) {
                        THdfsScanRange scanRange = scan.getScan_range().getHdfs_scan_range();
                        if (scanRange.isSetRelative_path() && fileName.contains(scanRange.relative_path)) {
                            sampled += 1;
                            return List.of(scan);
                        }
                        if (scanRange.isSetFull_path() && fileName.equals(scanRange.full_path)) {
                            sampled += 1;
                            return List.of(scan);
                        }
                    }
                }
                return List.of();
            }
        }
        return List.of();
    }
}