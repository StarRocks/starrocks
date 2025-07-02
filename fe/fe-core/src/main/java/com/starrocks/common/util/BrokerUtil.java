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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/BrokerUtil.java

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

package com.starrocks.common.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.thrift.TBrokerCheckPathExistRequest;
import com.starrocks.thrift.TBrokerCheckPathExistResponse;
import com.starrocks.thrift.TBrokerCloseReaderRequest;
import com.starrocks.thrift.TBrokerCloseWriterRequest;
import com.starrocks.thrift.TBrokerDeletePathRequest;
import com.starrocks.thrift.TBrokerFD;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TBrokerListPathRequest;
import com.starrocks.thrift.TBrokerListResponse;
import com.starrocks.thrift.TBrokerOpenMode;
import com.starrocks.thrift.TBrokerOpenReaderRequest;
import com.starrocks.thrift.TBrokerOpenReaderResponse;
import com.starrocks.thrift.TBrokerOpenWriterRequest;
import com.starrocks.thrift.TBrokerOpenWriterResponse;
import com.starrocks.thrift.TBrokerOperationStatus;
import com.starrocks.thrift.TBrokerOperationStatusCode;
import com.starrocks.thrift.TBrokerPReadRequest;
import com.starrocks.thrift.TBrokerPWriteRequest;
import com.starrocks.thrift.TBrokerReadResponse;
import com.starrocks.thrift.TBrokerRenamePathRequest;
import com.starrocks.thrift.TBrokerVersion;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;

public class BrokerUtil {
    private static final Logger LOG = LogManager.getLogger(BrokerUtil.class);

    private static int READ_BUFFER_SIZE_B = 1024 * 1024;

    /**
     * Parse file status in path with broker, except directory
     *
     * @param path
     * @param brokerDesc
     * @param fileStatuses: file path, size, isDir, isSplitable
     * @throws StarRocksException if broker op failed
     */
    public static void parseFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses)
            throws StarRocksException {
        TNetworkAddress address = getAddress(brokerDesc);
        try {
            TBrokerListPathRequest request = new TBrokerListPathRequest(
                    TBrokerVersion.VERSION_ONE, path, false, brokerDesc.getProperties());
            TBrokerListResponse tBrokerListResponse = ThriftRPCRequestExecutor.call(
                    ThriftConnectionPool.brokerPool,
                    address,
                    client -> client.listPath(request));

            if (tBrokerListResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new StarRocksException("Broker list path failed. path=" + path
                        + ",broker=" + address + ",msg=" + tBrokerListResponse.getOpStatus().getMessage());
            }
            for (TBrokerFileStatus tBrokerFileStatus : tBrokerListResponse.getFiles()) {
                if (tBrokerFileStatus.isDir) {
                    continue;
                }
                fileStatuses.add(tBrokerFileStatus);
            }
        } catch (TException e) {
            LOG.warn("Broker list path exception, path={}, address={}, exception={}", path, address, e);
            throw new StarRocksException("Broker list path exception. path=" + path +
                    ", broker=" + address + " exception: " + e.getMessage());
        }
    }

    public static String printBroker(String brokerName, TNetworkAddress address) {
        return brokerName + "[" + address.toString() + "]";
    }

    public static List<String> parseColumnsFromPath(String filePath, List<String> columnsFromPath)
            throws StarRocksException {
        if (columnsFromPath == null || columnsFromPath.isEmpty()) {
            return Collections.emptyList();
        }
        String[] strings = filePath.split("/");
        if (strings.length < 2) {
            throw new StarRocksException(
                    "Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
        }
        String[] columns = new String[columnsFromPath.size()];
        int size = 0;
        for (int i = strings.length - 2; i >= 0; i--) {
            String str = strings[i];
            if (str != null && str.isEmpty()) {
                continue;
            }
            if (str == null || !str.contains("=")) {
                throw new StarRocksException(
                        "Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            }
            String[] pair = str.split("=", 2);
            if (pair.length != 2) {
                throw new StarRocksException(
                        "Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            }
            int index = columnsFromPath.indexOf(pair[0]);
            if (index == -1) {
                continue;
            }
            columns[index] = pair[1];
            size++;
            if (size >= columnsFromPath.size()) {
                break;
            }
        }
        if (size != columnsFromPath.size()) {
            throw new StarRocksException(
                    "Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
        }
        return Lists.newArrayList(columns);
    }

    /**
     * Read binary data from path with broker
     *
     * @param path
     * @param brokerDesc
     * @return byte[]
     * @throws StarRocksException if broker op failed or not only one file
     */
    public static byte[] readFile(String path, BrokerDesc brokerDesc) throws StarRocksException {
        TNetworkAddress address = getAddress(brokerDesc);
        TBrokerFD fd = null;
        try {
            // get file size
            TBrokerListPathRequest request = new TBrokerListPathRequest(
                    TBrokerVersion.VERSION_ONE, path, false, brokerDesc.getProperties());
            TBrokerListResponse tBrokerListResponse = ThriftRPCRequestExecutor.call(
                    ThriftConnectionPool.brokerPool,
                    address,
                    client -> client.listPath(request));

            if (tBrokerListResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new StarRocksException("Broker list path failed. path=" + path + ", broker=" + address
                        + ",msg=" + tBrokerListResponse.getOpStatus().getMessage());
            }
            List<TBrokerFileStatus> fileStatuses = tBrokerListResponse.getFiles();
            if (fileStatuses.size() != 1) {
                throw new StarRocksException("Broker files num error. path=" + path + ", broker=" + address
                        + ", files num: " + fileStatuses.size());
            }

            Preconditions.checkState(!fileStatuses.get(0).isIsDir());
            long fileSize = fileStatuses.get(0).getSize();

            // open reader
            String clientId = NetUtils.getHostPortInAccessibleFormat(
                    FrontendOptions.getLocalHostAddress(), Config.rpc_port);
            TBrokerOpenReaderRequest tOpenReaderRequest = new TBrokerOpenReaderRequest(
                    TBrokerVersion.VERSION_ONE, path, 0, clientId, brokerDesc.getProperties());
            TBrokerOpenReaderResponse tOpenReaderResponse = ThriftRPCRequestExecutor.call(
                    ThriftConnectionPool.brokerPool,
                    address,
                    client -> client.openReader(tOpenReaderRequest));

            if (tOpenReaderResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new StarRocksException("Broker open reader failed. path=" + path + ", broker=" + address
                        + ", msg=" + tOpenReaderResponse.getOpStatus().getMessage());
            }
            fd = tOpenReaderResponse.getFd();

            // read
            TBrokerPReadRequest tPReadRequest = new TBrokerPReadRequest(
                    TBrokerVersion.VERSION_ONE, fd, 0, fileSize);
            TBrokerReadResponse tReadResponse = ThriftRPCRequestExecutor.call(
                    ThriftConnectionPool.brokerPool,
                    address,
                    client -> client.pread(tPReadRequest));

            if (tReadResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new StarRocksException("Broker read failed. path=" + path + ", broker=" + address
                        + ", msg=" + tReadResponse.getOpStatus().getMessage());
            }
            return tReadResponse.getData();
        } catch (TException e) {
            String failMsg = "Broker read file exception. path=" + path + ", broker=" + address;
            LOG.warn(failMsg, e);
            throw new StarRocksException(failMsg);
        } finally {
            // close reader
            if (fd != null) {
                TBrokerOperationStatus tOperationStatus = null;
                try {
                    TBrokerCloseReaderRequest tCloseReaderRequest = new TBrokerCloseReaderRequest(
                            TBrokerVersion.VERSION_ONE, fd);
                    tOperationStatus = ThriftRPCRequestExecutor.call(
                            ThriftConnectionPool.brokerPool,
                            address,
                            client -> client.closeReader(tCloseReaderRequest));
                } catch (TException e) {
                    LOG.warn("Broker close reader failed. path={}, address={}", path, address, e);
                }
                if (tOperationStatus == null) {
                    LOG.warn("Broker close reader failed: operation status is null. path={}, address={}", path, address);
                } else if (tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                    LOG.warn("Broker close reader failed. path={}, address={}, error={}", path, address,
                            tOperationStatus.getMessage());
                }
            }
        }
    }

    /**
     * Write binary data to destFilePath with broker
     *
     * @param data
     * @param destFilePath
     * @param brokerDesc
     * @throws StarRocksException if broker op failed
     */
    public static void writeFile(byte[] data, String destFilePath, BrokerDesc brokerDesc) throws StarRocksException {
        BrokerWriter writer = new BrokerWriter(destFilePath, brokerDesc);
        try {
            writer.open();
            ByteBuffer byteBuffer = ByteBuffer.wrap(data);
            writer.write(byteBuffer, data.length);
        } finally {
            writer.close();
        }
    }

    /**
     * Write srcFilePath file to destFilePath with broker
     *
     * @param srcFilePath
     * @param destFilePath
     * @param brokerDesc
     * @throws StarRocksException if broker op failed
     */
    public static void writeFile(String srcFilePath, String destFilePath,
                                 BrokerDesc brokerDesc) throws StarRocksException {
        BrokerWriter writer = new BrokerWriter(destFilePath, brokerDesc);
        ByteBuffer byteBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE_B);
        try (FileInputStream fis = new FileInputStream(srcFilePath); FileChannel channel = fis.getChannel()) {
            writer.open();
            while (true) {
                int readSize = channel.read(byteBuffer);
                if (readSize == -1) {
                    break;
                }

                byteBuffer.flip();
                writer.write(byteBuffer, readSize);
                byteBuffer.clear();
            }
        } catch (IOException e) {
            String failMsg = "Write file exception. filePath = " + srcFilePath
                    + ", destPath = " + destFilePath;
            LOG.warn(failMsg, e);
            throw new StarRocksException(failMsg);
        } finally {
            // close broker file writer and local file input stream
            writer.close();
        }
    }

    /**
     * Delete path with broker
     *
     * @param path
     * @param brokerDesc
     * @throws StarRocksException if broker op failed
     */
    public static void deletePath(String path, BrokerDesc brokerDesc) throws StarRocksException {
        TNetworkAddress address = getAddress(brokerDesc);
        try {
            TBrokerDeletePathRequest tDeletePathRequest = new TBrokerDeletePathRequest(
                    TBrokerVersion.VERSION_ONE, path, brokerDesc.getProperties());
            TBrokerOperationStatus tOperationStatus = ThriftRPCRequestExecutor.call(
                    ThriftConnectionPool.brokerPool,
                    address,
                    client -> client.deletePath(tDeletePathRequest));
            if (tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new StarRocksException("Broker delete path failed. path=" + path + ", broker=" + address
                        + ", msg=" + tOperationStatus.getMessage());
            }
        } catch (TException e) {
            LOG.warn("Broker read path exception, path={}, address={}, exception={}", path, address, e);
            throw new StarRocksException("Broker read path exception. path=" + path + ",broker=" + address);
        }
    }

    public static boolean checkPathExist(String remotePath, BrokerDesc brokerDesc) throws StarRocksException {
        TNetworkAddress address = getAddress(brokerDesc);
        try {
            TBrokerCheckPathExistRequest req = new TBrokerCheckPathExistRequest(TBrokerVersion.VERSION_ONE,
                    remotePath, brokerDesc.getProperties());
            TBrokerCheckPathExistResponse rep = ThriftRPCRequestExecutor.call(
                    ThriftConnectionPool.brokerPool,
                    address,
                    client -> client.checkPathExist(req));
            if (rep.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new StarRocksException("Broker check path exist failed. path=" + remotePath + ", broker=" + address +
                        ", msg=" + rep.getOpStatus().getMessage());
            }
            return rep.isPathExist;
        } catch (TException e) {
            LOG.warn("Broker check path exist failed, path={}, address={}, exception={}", remotePath, address, e);
            throw new StarRocksException("Broker check path exist exception. path=" + remotePath + ",broker=" + address);
        }
    }

    public static void rename(String origFilePath, String destFilePath, BrokerDesc brokerDesc) throws
            StarRocksException {
        rename(origFilePath, destFilePath, brokerDesc, Config.broker_client_timeout_ms);
    }

    public static void rename(String origFilePath, String destFilePath, BrokerDesc brokerDesc, int timeoutMs)
            throws StarRocksException {
        TNetworkAddress address = getAddress(brokerDesc);
        try {
            TBrokerRenamePathRequest req = new TBrokerRenamePathRequest(TBrokerVersion.VERSION_ONE, origFilePath,
                    destFilePath, brokerDesc.getProperties());
            TBrokerOperationStatus rep = ThriftRPCRequestExecutor.call(
                    ThriftConnectionPool.brokerPool,
                    address,
                    client -> client.renamePath(req));

            if (rep.getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new StarRocksException("failed to rename " + origFilePath + " to " + destFilePath +
                        ", msg: " + rep.getMessage() + ", broker: " + address);
            }
        } catch (TException e) {
            LOG.warn("Broker rename file failed, origin path={}, dest path={}, address={}, exception={}",
                    origFilePath, destFilePath, address, e);
            throw new StarRocksException("Broker rename file exception. origin path=" + origFilePath +
                    ", dest path=" + destFilePath + ", broker=" + address);
        }
    }

    private static TNetworkAddress getAddress(BrokerDesc brokerDesc) {
        String localIP = FrontendOptions.getLocalHostAddress();
        FsBroker broker = GlobalStateMgr.getCurrentState().getBrokerMgr().getBroker(brokerDesc.getName(), localIP);
        return new TNetworkAddress(broker.ip, broker.port);
    }

    private static class BrokerWriter {
        private String brokerFilePath;
        private BrokerDesc brokerDesc;
        private TNetworkAddress address;
        private TBrokerFD fd;
        private long currentOffset;
        private boolean isReady;

        public BrokerWriter(String brokerFilePath, BrokerDesc brokerDesc) {
            this.brokerFilePath = brokerFilePath;
            this.brokerDesc = brokerDesc;
            this.isReady = false;
        }

        public void open() throws StarRocksException {
            address = BrokerUtil.getAddress(brokerDesc);
            try {
                String clientId = NetUtils.getHostPortInAccessibleFormat(
                        FrontendOptions.getLocalHostAddress(), Config.rpc_port);
                TBrokerOpenWriterRequest tOpenWriterRequest = new TBrokerOpenWriterRequest(
                        TBrokerVersion.VERSION_ONE, brokerFilePath, TBrokerOpenMode.APPEND,
                        clientId, brokerDesc.getProperties());

                TBrokerOpenWriterResponse tOpenWriterResponse = ThriftRPCRequestExecutor.call(
                        ThriftConnectionPool.brokerPool,
                        address,
                        client -> client.openWriter(tOpenWriterRequest));

                if (tOpenWriterResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                    throw new StarRocksException("Broker open writer failed. destPath=" + brokerFilePath
                            + ", broker=" + address
                            + ", msg=" + tOpenWriterResponse.getOpStatus().getMessage());
                }
                fd = tOpenWriterResponse.getFd();
                currentOffset = 0L;
                isReady = true;
            } catch (TException e) {
                String failMsg = "Broker open writer exception. filePath=" + brokerFilePath + ", broker=" + address;
                LOG.warn(failMsg, e);
                throw new StarRocksException(failMsg);
            }
        }

        public void write(ByteBuffer byteBuffer, long bufferSize) throws StarRocksException {
            if (!isReady) {
                throw new StarRocksException(
                        "Broker writer is not ready. filePath=" + brokerFilePath + ", broker=" + address);
            }

            try {
                TBrokerPWriteRequest tPWriteRequest = new TBrokerPWriteRequest(
                        TBrokerVersion.VERSION_ONE, fd, currentOffset, byteBuffer);

                TBrokerOperationStatus tOperationStatus = ThriftRPCRequestExecutor.call(
                        ThriftConnectionPool.brokerPool,
                        address,
                        client -> client.pwrite(tPWriteRequest));

                if (tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                    throw new StarRocksException("Broker write failed. filePath=" + brokerFilePath + ", broker=" + address
                            + ", msg=" + tOperationStatus.getMessage());
                }
                currentOffset += bufferSize;
            } catch (TException e) {
                String failMsg = "Broker write exception. filePath=" + brokerFilePath + ", broker=" + address;
                LOG.warn(failMsg, e);
                throw new StarRocksException(failMsg);
            }
        }

        public void close() {
            // close broker writer
            if (fd != null) {
                TBrokerOperationStatus tOperationStatus = null;
                try {
                    TBrokerCloseWriterRequest tCloseWriterRequest = new TBrokerCloseWriterRequest(
                            TBrokerVersion.VERSION_ONE, fd);
                    tOperationStatus = ThriftRPCRequestExecutor.call(
                            ThriftConnectionPool.brokerPool,
                            address,
                            client -> client.closeWriter(tCloseWriterRequest));

                } catch (TException e) {
                    LOG.warn("Broker close writer failed. filePath={}, address={}", brokerFilePath, address, e);
                }
                if (tOperationStatus == null || tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                    String errMsg = (tOperationStatus == null) ?
                            "encounter exception when closing writer" : tOperationStatus.getMessage();
                    LOG.warn("Broker close writer failed. filePath={}, address={}, error={}", brokerFilePath,
                            address, errMsg);
                }
            }
            isReady = false;
        }
    }
}
