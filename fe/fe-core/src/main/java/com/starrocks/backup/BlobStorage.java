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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/backup/BlobStorage.java

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

package com.starrocks.backup;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.backup.Status.ErrCode;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.BrokerUtil;
import com.starrocks.common.util.NetUtils;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.fs.HdfsUtil.HdfsReader;
import com.starrocks.fs.HdfsUtil.HdfsWriter;
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
import org.apache.thrift.transport.TTransportException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class BlobStorage implements Writable {
    private static final Logger LOG = LogManager.getLogger(BlobStorage.class);

    @SerializedName("bn")
    private String brokerName;
    @SerializedName("pt")
    private Map<String, String> properties = Maps.newHashMap();
    @SerializedName("hasBroker")
    private boolean hasBroker;
    @SerializedName("brokerDesc")
    private BrokerDesc brokerDesc; // for non broker operation only

    private BlobStorage() {
        // for persist
    }

    public BlobStorage(String brokerName, Map<String, String> properties) {
        this.brokerName = brokerName;
        this.properties = properties;
        this.hasBroker = true;
    }

    public BlobStorage(String brokerName, Map<String, String> properties, boolean hasBroker) {
        this.brokerName = brokerName;
        this.properties = properties;
        this.hasBroker = hasBroker;
        if (!hasBroker) {
            brokerDesc = new BrokerDesc(properties);
        }
        if (this.brokerName == null) {
            this.brokerName = "";
        }
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean hasBroker() {
        return hasBroker;
    }

    public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
        if (!hasBroker) {
            return downloadWithFileSizeWithoutBroker(remoteFilePath, localFilePath, fileSize);
        }
        LOG.debug("download from {} to {}, file size: {}.",
                remoteFilePath, localFilePath, fileSize);

        long start = System.currentTimeMillis();

        // 1. get a proper broker
        TNetworkAddress brokerAddress = getBrokerAddress();

        // 2. open file reader with broker
        TBrokerFD fd = null;
        try {
            TBrokerOpenReaderRequest req = new TBrokerOpenReaderRequest(TBrokerVersion.VERSION_ONE, remoteFilePath,
                    0, clientId(), properties);

            TBrokerOpenReaderResponse rep = ThriftRPCRequestExecutor.callNoRetry(
                    ThriftConnectionPool.brokerPool,
                    brokerAddress,
                    client -> client.openReader(req));

            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to open reader on broker " + brokerName
                                + " for file: " + remoteFilePath + ". msg: " + opst.getMessage());
            }

            fd = rep.getFd();
            LOG.info("finished to open reader. fd: {}. download {} to {}.",
                    fd, remoteFilePath, localFilePath);
        } catch (TException e) {
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to open reader on broker " + brokerName
                            + " for file: " + remoteFilePath + ". msg: " + e.getMessage());
        }
        Preconditions.checkNotNull(fd);

        // 3. delete local file if exist
        File localFile = new File(localFilePath);
        if (localFile.exists()) {
            try {
                Files.walk(Paths.get(localFilePath),
                                FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                return new Status(ErrCode.COMMON_ERROR, "failed to delete exist local file: " + localFilePath);
            }
        }

        // 4. create local file
        Status status = Status.OK;
        try {
            if (!localFile.createNewFile()) {
                return new Status(ErrCode.COMMON_ERROR, "failed to create local file: " + localFilePath);
            }
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "failed to create local file: "
                    + localFilePath + ", msg: " + e.getMessage());
        }

        // 5. read remote file with broker and write to local
        String lastErrMsg = null;
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(localFile))) {
            final long bufSize = (long) 1024 * 1024; // 1MB
            long leftSize = fileSize;
            long readOffset = 0;
            while (leftSize > 0) {
                long readLen = Math.min(leftSize, bufSize);
                TBrokerReadResponse rep = null;
                // We only retry if we encounter a timeout thrift exception.
                int tryTimes = 0;
                while (tryTimes < 3) {
                    try {
                        TBrokerPReadRequest req = new TBrokerPReadRequest(TBrokerVersion.VERSION_ONE,
                                fd, readOffset, readLen);

                        rep = ThriftRPCRequestExecutor.callNoRetry(
                                ThriftConnectionPool.brokerPool,
                                brokerAddress,
                                client -> client.pread(req));

                        if (rep.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                            // pread return failure.
                            lastErrMsg = String.format("failed to read via broker %s. "
                                            + "current read offset: %d, read length: %d,"
                                            + " file size: %d, file: %s, err code: %d, msg: %s",
                                    brokerName,
                                    readOffset, readLen, fileSize,
                                    remoteFilePath, rep.getOpStatus().getStatusCode(),
                                    rep.getOpStatus().getMessage());
                            LOG.warn(lastErrMsg);
                            status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                        }
                        LOG.debug("download. readLen: {}, read data len: {}, left size:{}. total size: {}",
                                readLen, rep.getData().length, leftSize, fileSize);
                        break;
                    } catch (TTransportException e) {
                        if (e.getType() == TTransportException.TIMED_OUT) {
                            // we only retry when we encounter timeout exception.
                            lastErrMsg = String.format("failed to read via broker %s. "
                                            + "current read offset: %d, read length: %d,"
                                            + " file size: %d, file: %s, timeout.",
                                    brokerName,
                                    readOffset, readLen, fileSize,
                                    remoteFilePath);
                            tryTimes++;
                            continue;
                        }

                        lastErrMsg = String.format("failed to read via broker %s. "
                                        + "current read offset: %d, read length: %d,"
                                        + " file size: %d, file: %s. msg: %s",
                                brokerName,
                                readOffset, readLen, fileSize,
                                remoteFilePath, e.getMessage());
                        LOG.warn(lastErrMsg);
                        status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                        break;
                    } catch (TException e) {
                        lastErrMsg = String.format("failed to read via broker %s. "
                                        + "current read offset: %d, read length: %d,"
                                        + " file size: %d, file: %s. msg: %s",
                                brokerName,
                                readOffset, readLen, fileSize,
                                remoteFilePath, e.getMessage());
                        LOG.warn(lastErrMsg);
                        status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                        break;
                    }
                } // end of retry loop

                if (status.ok() && tryTimes < 3) {
                    // read succeed, write to local file
                    Preconditions.checkNotNull(rep);
                    // NOTICE(cmy): Sometimes the actual read length does not equal to the expected read length,
                    // even if the broker's read buffer size is large enough.
                    // I don't know why, but have to adapt to it.
                    if (rep.getData().length != readLen) {
                        LOG.warn("the actual read length does not equal to "
                                        + "the expected read length: {} vs. {}, file: {}, broker: {}",
                                rep.getData().length, readLen, remoteFilePath,
                                brokerName);
                    }

                    out.write(rep.getData());
                    readOffset += rep.getData().length;
                    leftSize -= rep.getData().length;
                } else {
                    status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                    break;
                }
            } // end of reading remote file
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Got exception: " + e.getMessage() + ", broker: " + brokerName);
        } finally {
            // close broker reader
            Status closeStatus = closeReader(brokerAddress, fd);
            if (!closeStatus.ok()) {
                LOG.warn(closeStatus.getErrMsg());
                if (status.ok()) {
                    // we return close write error only if no other error has been encountered.
                    status = closeStatus;
                }
            }
        }

        LOG.info("finished to download from {} to {} with size: {}. cost {} ms", remoteFilePath, localFilePath,
                fileSize, (System.currentTimeMillis() - start));
        return status;
    }

    public Status downloadWithFileSizeWithoutBroker(String remoteFilePath, String localFilePath, long fileSize) {
        LOG.debug("download from {} to {}, file size: {}.",
                remoteFilePath, localFilePath, fileSize);

        long start = System.currentTimeMillis();
        // 1. open remote file
        HdfsReader reader = HdfsUtil.openHdfsReader(remoteFilePath, brokerDesc);
        if (reader == null) {
            return new Status(ErrCode.COMMON_ERROR, "fail to open reader for " + remoteFilePath);
        }
        // 2. delete local file if exist
        File localFile = new File(localFilePath);
        if (localFile.exists()) {
            try {
                Files.walk(Paths.get(localFilePath),
                                FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                return new Status(ErrCode.COMMON_ERROR, "failed to delete exist local file: " + localFilePath);
            }
        }

        // 3. create local file
        Status status = Status.OK;
        try {
            if (!localFile.createNewFile()) {
                return new Status(ErrCode.COMMON_ERROR, "failed to create local file: " + localFilePath);
            }
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "failed to create local file: "
                    + localFilePath + ", msg: " + e.getMessage());
        }

        // 4. read remote file with broker and write to local
        String lastErrMsg = null;
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(localFile))) {
            final long bufSize = (long) 1024 * 1024; // 1MB
            long leftSize = fileSize;
            long readOffset = 0;
            while (leftSize > 0) {
                long readLen = Math.min(leftSize, bufSize);
                byte[] readData;
                try {
                    readData = reader.read(readLen);
                } catch (StarRocksException e) {
                    lastErrMsg = String.format("failed to read. "
                                    + "current read offset: %d, read length: %d,"
                                    + " file size: %d, file: %s. msg: %s",
                            readOffset, readLen, fileSize,
                            remoteFilePath, e.getMessage());
                    LOG.warn(lastErrMsg);
                    status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                    break;
                }

                // Sometimes the actual read length does not equal to the expected read length, 
                // even if the broker's read buffer size is large enough. 
                // So here we advance the readOffSet by actual read length (readData.length)
                out.write(readData);
                readOffset += readData.length;
                leftSize -= readData.length;
            }
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Got exception: " + e.getMessage());
        } finally {
            reader.close();
        }

        LOG.info("finished to download from {} to {} with size: {}. cost {} ms", remoteFilePath, localFilePath,
                fileSize, (System.currentTimeMillis() - start));
        return status;
    }

    // directly upload the content to remote file
    public Status directUpload(String content, String remoteFile) {
        if (!hasBroker) {
            return directUploadWithoutBroker(content, remoteFile);
        }
        Status status = Status.OK;
        // 1. get a proper broker
        TNetworkAddress brokerAddress = getBrokerAddress();

        TBrokerFD fd = new TBrokerFD();
        try {
            // 2. open file writer with broker
            status = openWriter(brokerAddress, remoteFile, fd);
            if (!status.ok()) {
                return status;
            }

            // 3. write content
            try {
                ByteBuffer bb = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
                TBrokerPWriteRequest req = new TBrokerPWriteRequest(TBrokerVersion.VERSION_ONE, fd, 0, bb);

                TBrokerOperationStatus opst = ThriftRPCRequestExecutor.callNoRetry(
                        ThriftConnectionPool.brokerPool,
                        brokerAddress,
                        client -> client.pwrite(req));

                if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                    // pwrite return failure.
                    status = new Status(ErrCode.COMMON_ERROR, "write failed: " + opst.getMessage()
                            + ", broker: " + brokerName);
                }
            } catch (TException e) {
                status = new Status(ErrCode.BAD_CONNECTION, "write exception: " + e.getMessage()
                        + ", broker: " + brokerName);
            }
        } finally {
            closeWriter(brokerAddress, fd);
        }

        return status;
    }

    public Status directUploadWithoutBroker(String content, String remoteFile) {
        Status status = Status.OK;
        try {
            HdfsUtil.writeFile(content.getBytes(StandardCharsets.UTF_8), remoteFile, brokerDesc);
        } catch (StarRocksException e) {
            status = new Status(ErrCode.BAD_CONNECTION, "write exception: " + e.getMessage());
        }
        return status;
    }

    public Status upload(String localPath, String remotePath) {
        if (!hasBroker) {
            return uploadWithoutBroker(localPath, remotePath);
        }
        long start = System.currentTimeMillis();

        Status status = Status.OK;
        // 1. get a proper broker
        TNetworkAddress brokerAddress = getBrokerAddress();

        // 2. open file write with broker
        TBrokerFD fd = new TBrokerFD();
        status = openWriter(brokerAddress, remotePath, fd);
        if (!status.ok()) {
            return status;
        }

        // 3. read local file and write to remote with broker
        File localFile = new File(localPath);
        long fileLength = localFile.length();
        byte[] readBuf = new byte[1024];
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(localFile))) {
            // save the last err msg
            String lastErrMsg = null;
            // save the current write offset of remote file
            long writeOffset = 0;
            // read local file, 1MB at a time
            int bytesRead = 0;
            while ((bytesRead = in.read(readBuf)) != -1) {
                ByteBuffer bb = ByteBuffer.wrap(readBuf, 0, bytesRead);

                // We only retry if we encounter a timeout thrift exception.
                int tryTimes = 0;
                while (tryTimes < 3) {
                    try {
                        TBrokerPWriteRequest req =
                                new TBrokerPWriteRequest(TBrokerVersion.VERSION_ONE, fd, writeOffset, bb);

                        TBrokerOperationStatus opst = ThriftRPCRequestExecutor.callNoRetry(
                                ThriftConnectionPool.brokerPool,
                                brokerAddress,
                                client -> client.pwrite(req));

                        if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                            // pwrite return failure.
                            lastErrMsg = String.format("failed to write via broker %s. "
                                            + "current write offset: %d, write length: %d,"
                                            + " file length: %d, file: %s, err code: %d, msg: %s",
                                    brokerName,
                                    writeOffset, bytesRead, fileLength,
                                    remotePath, opst.getStatusCode(), opst.getMessage());
                            LOG.warn(lastErrMsg);
                            status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                        }
                        break;
                    } catch (TTransportException e) {
                        if (e.getType() == TTransportException.TIMED_OUT) {
                            // we only retry when we encounter timeout exception.
                            lastErrMsg = String.format("failed to write via broker %s. "
                                            + "current write offset: %d, write length: %d,"
                                            + " file length: %d, file: %s. timeout",
                                    brokerName,
                                    writeOffset, bytesRead, fileLength,
                                    remotePath);
                            tryTimes++;
                            continue;
                        }

                        lastErrMsg = String.format("failed to write via broker %s. "
                                        + "current write offset: %d, write length: %d,"
                                        + " file length: %d, file: %s. encounter TTransportException: %s",
                                brokerName,
                                writeOffset, bytesRead, fileLength,
                                remotePath, e.getMessage());
                        LOG.warn(lastErrMsg, e);
                        status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                        break;
                    } catch (TException e) {
                        lastErrMsg = String.format("failed to write via broker %s. "
                                        + "current write offset: %d, write length: %d,"
                                        + " file length: %d, file: %s. encounter TException: %s",
                                brokerName,
                                writeOffset, bytesRead, fileLength,
                                remotePath, e.getMessage());
                        LOG.warn(lastErrMsg, e);
                        status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                        break;
                    }
                }

                if (status.ok() && tryTimes < 3) {
                    // write succeed, update current write offset
                    writeOffset += bytesRead;
                } else {
                    status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                    break;
                }
            } // end of read local file loop
        } catch (FileNotFoundException e1) {
            return new Status(ErrCode.COMMON_ERROR, "encounter file not found exception: " + e1.getMessage()
                    + ", broker: " + brokerName);
        } catch (IOException e1) {
            return new Status(ErrCode.COMMON_ERROR, "encounter io exception: " + e1.getMessage()
                    + ", broker: " + brokerName);
        } finally {
            // close write
            Status closeStatus = closeWriter(brokerAddress, fd);
            if (!closeStatus.ok()) {
                LOG.warn(closeStatus.getErrMsg());
                if (status.ok()) {
                    // we return close write error only if no other error has been encountered.
                    status = closeStatus;
                }
            }
        }

        if (status.ok()) {
            LOG.info("finished to upload {} to remote path {}. cost: {} ms",
                    localPath, remotePath, (System.currentTimeMillis() - start));
        }
        return status;
    }

    public Status uploadWithoutBroker(String localPath, String remotePath) {
        long start = System.currentTimeMillis();

        Status status = Status.OK;

        HdfsWriter writer = HdfsUtil.openHdfsWriter(remotePath, brokerDesc);
        if (writer == null) {
            return new Status(ErrCode.COMMON_ERROR, "fail to open writer for " + remotePath);
        }
        File localFile = new File(localPath);
        long fileLength = localFile.length();
        byte[] readBuf = new byte[1024];
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(localFile))) {
            // save the last err msg
            String lastErrMsg = null;
            // save the current write offset of remote file
            long writeOffset = 0;
            // read local file, 1MB at a time
            int bytesRead = 0;
            while ((bytesRead = in.read(readBuf)) != -1) {
                ByteBuffer bb = ByteBuffer.wrap(readBuf, 0, bytesRead);

                try {
                    writer.write(bb, bytesRead);
                } catch (StarRocksException e) {
                    lastErrMsg = String.format("failed to write. "
                                    + "current write offset: %d, write length: %d,"
                                    + " file length: %d, file: %s. encounter TException: %s",
                            writeOffset, bytesRead, fileLength,
                            remotePath, e.getMessage());
                    LOG.warn(lastErrMsg, e);
                    status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                    break;
                }
                // write succeed, update current write offset
                writeOffset += bytesRead;
            }
        } catch (FileNotFoundException e1) {
            return new Status(ErrCode.COMMON_ERROR, "encounter file not found exception: " + e1.getMessage());
        } catch (IOException e1) {
            return new Status(ErrCode.COMMON_ERROR, "encounter io exception: " + e1.getMessage());
        } finally {
            // close write
            writer.close();
        }

        if (status.ok()) {
            LOG.info("finished to upload {} to remote path {}. cost: {} ms",
                    localPath, remotePath, (System.currentTimeMillis() - start));
        }
        return status;
    }

    public Status rename(String origFilePath, String destFilePath) {
        if (!hasBroker) {
            return renameWithoutBroker(origFilePath, destFilePath);
        }
        long start = System.currentTimeMillis();

        // 1. get a proper broker
        TNetworkAddress brokerAddress = getBrokerAddress();

        // 2. rename
        try {
            TBrokerRenamePathRequest req = new TBrokerRenamePathRequest(TBrokerVersion.VERSION_ONE, origFilePath,
                    destFilePath, properties);
            TBrokerOperationStatus ost = ThriftRPCRequestExecutor.callNoRetry(
                    ThriftConnectionPool.brokerPool,
                    brokerAddress,
                    client -> client.renamePath(req));
            if (ost.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to rename " + origFilePath + " to " + destFilePath + ", msg: " + ost.getMessage()
                                + ", broker: " + brokerName);
            }
        } catch (TException e) {
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to rename " + origFilePath + " to " + destFilePath + ", msg: " + e.getMessage()
                            + ", broker: " + brokerName);
        }

        LOG.info("finished to rename {} to  {}. cost: {} ms",
                origFilePath, destFilePath, (System.currentTimeMillis() - start));
        return Status.OK;
    }

    public Status renameWithoutBroker(String origFilePath, String destFilePath) {
        long start = System.currentTimeMillis();

        try {
            HdfsUtil.rename(origFilePath, destFilePath, brokerDesc);
        } catch (StarRocksException e) {
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to rename " + origFilePath + " to " + destFilePath + ", msg: " + e.getMessage());
        }

        LOG.info("finished to rename {} to  {}. cost: {} ms",
                origFilePath, destFilePath, (System.currentTimeMillis() - start));
        return Status.OK;
    }

    public Status delete(String remotePath) {
        if (!hasBroker) {
            return deleteWithoutBroker(remotePath);
        }

        TNetworkAddress brokerAddress = getBrokerAddress();

        // delete
        try {
            TBrokerDeletePathRequest req = new TBrokerDeletePathRequest(TBrokerVersion.VERSION_ONE, remotePath,
                    properties);
            TBrokerOperationStatus opst = ThriftRPCRequestExecutor.callNoRetry(
                    ThriftConnectionPool.brokerPool,
                    brokerAddress,
                    client -> client.deletePath(req));

            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to delete remote path: " + remotePath + ". msg: " + opst.getMessage()
                                + ", broker: " + brokerName);
            }

            LOG.info("finished to delete remote path {}.", remotePath);
        } catch (TException e) {
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to delete remote path: " + remotePath + ". msg: " + e.getMessage()
                            + ", broker: " + brokerName);
        }

        return Status.OK;
    }

    public Status deleteWithoutBroker(String remotePath) {
        try {
            HdfsUtil.deletePath(remotePath, this.brokerDesc);
            LOG.info("finished to delete remote path {}.", remotePath);
        } catch (StarRocksException e) {
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to delete remote path: " + remotePath + ". msg: " + e.getMessage());
        }
        return Status.OK;
    }

    // List files in remotePath
    // The remote file name will only contains file name only(Not full path)
    public Status list(String remotePath, List<RemoteFile> result) {
        if (!hasBroker) {
            return listWithoutBroker(remotePath, result);
        }

        TNetworkAddress brokerAddress = getBrokerAddress();

        // list
        try {
            TBrokerListPathRequest req = new TBrokerListPathRequest(TBrokerVersion.VERSION_ONE, remotePath,
                    false /* not recursive */, properties);
            req.setFileNameOnly(true);

            TBrokerListResponse rep = ThriftRPCRequestExecutor.callNoRetry(
                    ThriftConnectionPool.brokerPool,
                    brokerAddress,
                    client -> client.listPath(req));

            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to list remote path: " + remotePath + ". msg: " + opst.getMessage()
                                + ", broker: " + brokerName);
            }

            List<TBrokerFileStatus> fileStatus = rep.getFiles();
            for (TBrokerFileStatus tFile : fileStatus) {
                RemoteFile file = new RemoteFile(tFile.path, !tFile.isDir, tFile.size);
                result.add(file);
            }
            LOG.info("finished to list remote path {}. get files: {}", remotePath, result);
        } catch (TException e) {
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to list remote path: " + remotePath + ". msg: " + e.getMessage()
                            + ", broker: " + brokerName);
        }

        return Status.OK;
    }

    public Status listWithoutBroker(String remotePath, List<RemoteFile> result) {
        try {
            List<TBrokerFileStatus> fileStatus = Lists.newArrayList();
            HdfsUtil.parseFile(remotePath, this.brokerDesc, fileStatus, false, true);
            for (TBrokerFileStatus tFile : fileStatus) {
                RemoteFile file = new RemoteFile(tFile.path, !tFile.isDir, tFile.size);
                result.add(file);
            }
            LOG.info("finished to list remote path {}. get files: {}", remotePath, result);
        } catch (StarRocksException e) {
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to list remote path: " + remotePath + ". msg: " + e.getMessage());
        }

        return Status.OK;
    }

    public Status checkPathExist(String remotePath) {
        if (!hasBroker) {
            return checkPathExistWithoutBroker(remotePath);
        }
        TNetworkAddress address = getBrokerAddress();

        // check path
        try {
            TBrokerCheckPathExistRequest req = new TBrokerCheckPathExistRequest(TBrokerVersion.VERSION_ONE,
                    remotePath, properties);

            TBrokerCheckPathExistResponse rep = ThriftRPCRequestExecutor.callNoRetry(
                    ThriftConnectionPool.brokerPool,
                    address,
                    client -> client.checkPathExist(req));

            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to check remote path exist: " + remotePath
                                + ", broker: " + brokerName
                                + ". msg: " + opst.getMessage());
            }

            if (!rep.isIsPathExist()) {
                return new Status(ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
            }

            return Status.OK;
        } catch (TException e) {
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to check remote path exist: " + remotePath
                            + ", broker: " + brokerName
                            + ". msg: " + e.getMessage());
        }
    }

    public Status checkPathExistWithoutBroker(String remotePath) {
        try {
            boolean exist = HdfsUtil.checkPathExist(remotePath, this.brokerDesc);
            if (!exist) {
                return new Status(ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
            }
            return Status.OK;
        } catch (StarRocksException e) {
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to check remote path exist: " + remotePath
                            + ". msg: " + e.getMessage());
        }
    }

    public static String clientId() {
        return NetUtils.getHostPortInAccessibleFormat(FrontendOptions.getLocalHostAddress(), Config.edit_log_port);
    }

    private TNetworkAddress getBrokerAddress() {
        String localIP = FrontendOptions.getLocalHostAddress();
        FsBroker broker = GlobalStateMgr.getCurrentState().getBrokerMgr().getBroker(brokerName, localIP);
        TNetworkAddress address = new TNetworkAddress(broker.ip, broker.port);
        LOG.info("get broker: {}", BrokerUtil.printBroker(brokerName, address));
        return address;
    }

    private Status openWriter(TNetworkAddress address, String remoteFile, TBrokerFD fd) {
        try {
            TBrokerOpenWriterRequest req = new TBrokerOpenWriterRequest(TBrokerVersion.VERSION_ONE,
                    remoteFile, TBrokerOpenMode.APPEND, clientId(), properties);

            TBrokerOpenWriterResponse rep = ThriftRPCRequestExecutor.callNoRetry(
                    ThriftConnectionPool.brokerPool,
                    address,
                    client -> client.openWriter(req));

            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to open writer on broker " + BrokerUtil.printBroker(brokerName, address)
                                + " for file: " + remoteFile + ". msg: " + opst.getMessage());
            }

            fd.setHigh(rep.getFd().getHigh());
            fd.setLow(rep.getFd().getLow());
            LOG.info("finished to open writer. fd: {}. directly upload to remote path {}.",
                    fd, remoteFile);
        } catch (TException e) {
            return new Status(ErrCode.BAD_CONNECTION,
                    "failed to open writer on broker " + BrokerUtil.printBroker(brokerName, address)
                            + ", err: " + e.getMessage());
        }

        return Status.OK;
    }

    private Status closeWriter(TNetworkAddress address, TBrokerFD fd) {
        try {
            TBrokerCloseWriterRequest req = new TBrokerCloseWriterRequest(TBrokerVersion.VERSION_ONE, fd);

            TBrokerOperationStatus st = ThriftRPCRequestExecutor.callNoRetry(
                    ThriftConnectionPool.brokerPool,
                    address,
                    client -> client.closeWriter(req));

            if (st.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to close writer on broker " + BrokerUtil.printBroker(brokerName, address)
                                + " for fd: " + fd);
            }

            LOG.info("finished to close writer. fd: {}.", fd);
        } catch (TException e) {
            return new Status(ErrCode.BAD_CONNECTION,
                    "failed to close writer on broker " + BrokerUtil.printBroker(brokerName, address)
                            + ", fd " + fd + ", msg: " + e.getMessage());
        }

        return Status.OK;
    }

    private Status closeReader(TNetworkAddress address, TBrokerFD fd) {
        try {
            TBrokerCloseReaderRequest req = new TBrokerCloseReaderRequest(TBrokerVersion.VERSION_ONE, fd);

            TBrokerOperationStatus st = ThriftRPCRequestExecutor.callNoRetry(
                    ThriftConnectionPool.brokerPool,
                    address,
                    client -> client.closeReader(req));

            if (st.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to close reader on broker " + BrokerUtil.printBroker(brokerName, address)
                                + " for fd: " + fd);
            }

            LOG.info("finished to close reader. fd: {}.", fd);
        } catch (TException e) {
            return new Status(ErrCode.BAD_CONNECTION,
                    "failed to close reader on broker " + BrokerUtil.printBroker(brokerName, address)
                            + ", fd " + fd + ", msg: " + e.getMessage());
        }

        return Status.OK;
    }

    public static BlobStorage read(DataInput in) throws IOException {
        BlobStorage blobStorage = new BlobStorage();
        blobStorage.readFields(in);
        return blobStorage;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // must write type first
        Text.writeString(out, brokerName);

        out.writeInt(properties.size() + 1);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
        Text.writeString(out, "HasBrokerField");
        Text.writeString(out, String.valueOf(hasBroker));
    }

    public void readFields(DataInput in) throws IOException {
        brokerName = Text.readString(in);

        // properties
        int size = in.readInt();
        hasBroker = true;
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            if (key.equals("HasBrokerField")) {
                hasBroker = Boolean.valueOf(value);
                continue;
            }
            properties.put(key, value);
        }
        if (!hasBroker) {
            brokerDesc = new BrokerDesc(properties);
        }
    }
}
