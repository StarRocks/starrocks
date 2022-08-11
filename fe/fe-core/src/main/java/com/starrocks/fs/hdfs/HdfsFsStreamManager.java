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

import com.starrocks.thrift.TBrokerFD;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

public class HdfsFsStreamManager {
    private static final Logger LOG = LogManager.getLogger(HdfsFsStreamManager.class);

    private HashMap<TBrokerFD, BrokerInputStream> inputStreams;
    private HashMap<TBrokerFD, BrokerOutputStream> outputStreams;

    public HdfsFsStreamManager() {
        inputStreams = new HashMap<>();
        outputStreams = new HashMap<>();
    }

    public synchronized void putNewOutputStream(TBrokerFD fd, FSDataOutputStream fsDataOutputStream,
                                                HdfsFs brokerFileSystem) {
        outputStreams.putIfAbsent(fd, new BrokerOutputStream(fsDataOutputStream, brokerFileSystem));
    }

    public synchronized void putNewInputStream(TBrokerFD fd, FSDataInputStream fsDataInputStream,
                                               HdfsFs brokerFileSystem) {
        inputStreams.putIfAbsent(fd, new BrokerInputStream(fsDataInputStream, brokerFileSystem));
    }

    public synchronized FSDataInputStream getFsDataInputStream(TBrokerFD fd) {
        BrokerInputStream brokerInputStream = inputStreams.get(fd);
        if (brokerInputStream != null) {
            return brokerInputStream.getInputStream();
        }
        return null;
    }

    public synchronized FSDataOutputStream getFsDataOutputStream(TBrokerFD fd) {
        BrokerOutputStream brokerOutputStream = outputStreams.get(fd);
        if (brokerOutputStream != null) {
            return brokerOutputStream.getOutputStream();
        }
        return null;
    }

    public synchronized void removeInputStream(TBrokerFD fd) {
        BrokerInputStream brokerInputStream = inputStreams.remove(fd);
        try {
            if (brokerInputStream != null) {
                brokerInputStream.updateAccessTime();
                brokerInputStream.inputStream.close();
            }
        } catch (Exception e) {
            LOG.error("errors while close file data input stream", e);
        }
    }

    public synchronized void removeOutputStream(TBrokerFD fd) {
        BrokerOutputStream brokerOutputStream = outputStreams.remove(fd);
        try {
            if (brokerOutputStream != null) {
                brokerOutputStream.updateAccessTime();
                brokerOutputStream.outputStream.close();
            }
        } catch (Exception e) {
            LOG.error("errors while close file data output stream", e);
        }
    }

    private static class BrokerOutputStream {

        private final FSDataOutputStream outputStream;
        private final HdfsFs brokerFileSystem;

        public BrokerOutputStream(FSDataOutputStream outputStream, HdfsFs brokerFileSystem) {
            this.outputStream = outputStream;
            this.brokerFileSystem = brokerFileSystem;
            this.brokerFileSystem.updateLastUpdateAccessTime();
        }

        public FSDataOutputStream getOutputStream() {
            this.brokerFileSystem.updateLastUpdateAccessTime();
            return outputStream;
        }

        public void updateAccessTime() {
            this.brokerFileSystem.updateLastUpdateAccessTime();
        }
    }

    private static class BrokerInputStream {

        private final FSDataInputStream inputStream;
        private final HdfsFs brokerFileSystem;

        public BrokerInputStream(FSDataInputStream inputStream, HdfsFs brokerFileSystem) {
            this.inputStream = inputStream;
            this.brokerFileSystem = brokerFileSystem;
            this.brokerFileSystem.updateLastUpdateAccessTime();
        }

        public FSDataInputStream getInputStream() {
            this.brokerFileSystem.updateLastUpdateAccessTime();
            return inputStream;
        }

        public void updateAccessTime() {
            this.brokerFileSystem.updateLastUpdateAccessTime();
        }
    }
}
