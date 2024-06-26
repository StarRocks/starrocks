// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.fs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public class HdfsFs {

    private static Logger LOG = LogManager.getLogger(HdfsFs.class);

    private ReentrantLock lock;
    private HdfsFsIdentity identity;
    private FileSystem dfsFileSystem;
    private long lastAccessTimestamp;
    private UUID fileSystemId;
    private Configuration configuration;
    private String userName;

    public HdfsFs() {

    }

    public HdfsFs(HdfsFsIdentity identity) {
        this.identity = identity;
        this.lock = new ReentrantLock();
        this.dfsFileSystem = null;
        this.lastAccessTimestamp = System.currentTimeMillis();
        this.fileSystemId = UUID.randomUUID();
        this.configuration = null;
        this.userName = null;
    }

    public synchronized void setFileSystem(FileSystem fileSystem) {
        this.dfsFileSystem = fileSystem;
        this.lastAccessTimestamp = System.currentTimeMillis();
    }

    public synchronized void setUserName(String userName) {
        this.userName = userName;
        this.lastAccessTimestamp = System.currentTimeMillis();
    }

    public synchronized void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        this.lastAccessTimestamp = System.currentTimeMillis();
    }

    public void closeFileSystem() {
        lock.lock();
        try {
            if (this.dfsFileSystem != null) {
                try {
                    this.dfsFileSystem.close();
                } catch (Exception e) {
                    LOG.error("errors while close file system", e);
                } finally {
                    this.dfsFileSystem = null;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public synchronized FileSystem getDFSFileSystem() {
        this.lastAccessTimestamp = System.currentTimeMillis();
        return dfsFileSystem;
    }

    public synchronized String getUserName() {
        this.lastAccessTimestamp = System.currentTimeMillis();
        return userName;
    }

    public synchronized Configuration getConfiguration() {
        this.lastAccessTimestamp = System.currentTimeMillis();
        return configuration;
    }

    public void updateLastUpdateAccessTime() {
        this.lastAccessTimestamp = System.currentTimeMillis();
    }

    public HdfsFsIdentity getIdentity() {
        return identity;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public boolean isExpired(long expirationIntervalSecs) {
        if (System.currentTimeMillis() - lastAccessTimestamp > expirationIntervalSecs * 1000) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "HDFSFileSystem [identity=" + identity + ", dfsFileSystem="
                + dfsFileSystem + ", fileSystemId=" + fileSystemId + "]";
    }
}
