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

package com.starrocks.connector.jdbc;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class JDBCDriverEntry {
    private String name;
    private String checksum;
    private Long firstAccessTs = Long.MAX_VALUE;
    private String location;
    private final AtomicBoolean isAvailable = new AtomicBoolean(false);
    private final AtomicBoolean shouldDelete = new AtomicBoolean(false);
    private final Lock downloadLock = new ReentrantLock();
    boolean isDownloaded = false;

    public JDBCDriverEntry(String name, String checksum) {
        this.name = name;
        this.checksum = checksum;
    }

    public boolean isExpected(String name, String checksum) {
        return Objects.equals(this.name, name) && Objects.equals(this.checksum, checksum);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getChecksum() {
        return checksum;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public Long getFirstAccessTs() {
        return firstAccessTs;
    }

    public void setFirstAccessTs(Long firstAccessTs) {
        this.firstAccessTs = firstAccessTs;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public boolean isAvailable() {
        return isAvailable.get();
    }

    public void setAvailable(boolean isAvailable) {
        this.isAvailable.set(isAvailable);
    }

    public boolean shouldDelete() {
        return shouldDelete.get();
    }

    public void setShouldDelete(boolean shouldDelete) {
        this.shouldDelete.set(shouldDelete);
    }

    public boolean isDownloaded() {
        return isDownloaded;
    }

    public void setDownloaded(boolean isDownloaded) {
        this.isDownloaded = isDownloaded;
    }

    public Lock getDownloadLock() {
        return downloadLock;
    }
}
