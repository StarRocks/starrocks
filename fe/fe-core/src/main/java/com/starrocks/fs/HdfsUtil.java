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

package com.starrocks.fs;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.ClientPool;
import com.starrocks.common.UserException;
import com.starrocks.fs.hdfs.HdfsService;
import com.starrocks.thrift.TBrokerCheckPathExistRequest;
import com.starrocks.thrift.TBrokerCloseReaderRequest;
import com.starrocks.thrift.TBrokerCloseWriterRequest;
import com.starrocks.thrift.TBrokerDeletePathRequest;
import com.starrocks.thrift.TBrokerFD;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TBrokerListPathRequest;
import com.starrocks.thrift.TBrokerOpenMode;
import com.starrocks.thrift.TBrokerOpenReaderRequest;
import com.starrocks.thrift.TBrokerOpenWriterRequest;
import com.starrocks.thrift.TBrokerPReadRequest;
import com.starrocks.thrift.TBrokerPWriteRequest;
import com.starrocks.thrift.TBrokerRenamePathRequest;
import com.starrocks.thrift.TBrokerVersion;
import com.starrocks.thrift.THdfsProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;

public class HdfsUtil {
    private static final Logger LOG = LogManager.getLogger(HdfsUtil.class);

    private static int READ_BUFFER_SIZE_B = 1024 * 1024;

    private static HdfsService hdfsService = new HdfsService();

    
    public static void getTProperties(String path, BrokerDesc brokerDesc,  THdfsProperties tProperties) throws UserException {
        hdfsService.getTProperties(path, brokerDesc.getProperties(), tProperties);
    }

    /**
     * Parse file status in path with broker, except directory
     *
     * @param path
     * @param brokerDesc
     * @param fileStatuses: file path, size, isDir, isSplitable
     * @throws UserException if broker op failed
     */
    public static void parseFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses, boolean skipDir,
                                 boolean fileNameOnly) throws UserException {
        if (path.startsWith(TableFunctionTable.FAKE_PATH)) {
            fileStatuses.add(new TBrokerFileStatus("file1", false, 1024, false));
            return;
        }
        TBrokerListPathRequest request = new TBrokerListPathRequest(
                TBrokerVersion.VERSION_ONE, path, false, brokerDesc.getProperties());
        hdfsService.listPath(request, fileStatuses, skipDir, fileNameOnly);
    }

    public static List<FileStatus> listFileMeta(String path, BrokerDesc brokerDesc) throws UserException {
        if (path.startsWith(TableFunctionTable.FAKE_PATH)) {
            path = StringUtils.removeStart(path, TableFunctionTable.FAKE_PATH);
            FileStatus fakeFile = new FileStatus(1, false, 1, 1024, System.currentTimeMillis(), new Path(path));
            return Lists.newArrayList(fakeFile);
        }
        return hdfsService.listFileMeta(path, brokerDesc.getProperties(), true);
    }

    public static void parseFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses)
            throws UserException {
        parseFile(path, brokerDesc, fileStatuses, true, false);
    }

    public static List<String> parseColumnsFromPath(String filePath, List<String> columnsFromPath)
            throws UserException {
        if (columnsFromPath == null || columnsFromPath.isEmpty()) {
            return Collections.emptyList();
        }
        String[] strings = filePath.split("/");
        if (strings.length < 2) {
            throw new UserException(
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
                throw new UserException(
                        "Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            }
            String[] pair = str.split("=", 2);
            if (pair.length != 2) {
                throw new UserException(
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
            throw new UserException(
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
     * @throws UserException if broker op failed or not only one file
     */
    public static byte[] readFile(String path, BrokerDesc brokerDesc) throws UserException {
        HdfsReader reader = new HdfsReader(path, brokerDesc);
        try {
            reader.open();
            long fileSize = reader.getFileSize();
            byte[] result = reader.read(fileSize);
            return result;
        } finally {
            reader.close();
        }
    }

    public static HdfsReader openHdfsReader(String path, BrokerDesc brokerDesc) {
        HdfsReader reader = new HdfsReader(path, brokerDesc);
        try {
            reader.open();
        } catch (UserException e) {
            reader.close();
            return null;
        }
        return reader;
    }

    /**
     * Write binary data to destFilePath with broker
     *
     * @param data
     * @param destFilePath
     * @param brokerDesc
     * @throws UserException if broker op failed
     */
    public static void writeFile(byte[] data, String destFilePath, BrokerDesc brokerDesc) throws UserException {
        HdfsWriter writer = new HdfsWriter(destFilePath, brokerDesc);
        try {
            writer.open();
            ByteBuffer byteBuffer = ByteBuffer.wrap(data);
            writer.write(byteBuffer, data.length);
        } finally {
            writer.close();
        }
    }

    public static HdfsWriter openHdfsWriter(String destFilePath, BrokerDesc brokerDesc) {
        HdfsWriter writer = new HdfsWriter(destFilePath, brokerDesc);
        try {
            writer.open();
        } catch (UserException e) {
            writer.close();
            return null;
        }
        return writer;
    }

    /**
     * Write srcFilePath file to destFilePath with broker
     *
     * @param srcFilePath
     * @param destFilePath
     * @param brokerDesc
     * @throws UserException if broker op failed
     */
    public static void writeFile(String srcFilePath, String destFilePath,
            BrokerDesc brokerDesc) throws UserException {
        HdfsWriter writer = new HdfsWriter(destFilePath, brokerDesc);
        ByteBuffer byteBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE_B);
        try (FileInputStream inputFs = new FileInputStream(srcFilePath); FileChannel channel = inputFs.getChannel()) {
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
            String failMsg = "Write file exception. srcPath = " + srcFilePath + 
                    ", destPath = " + destFilePath;
            LOG.warn(failMsg, e);
            throw new UserException(failMsg);
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
     * @throws UserException if broker op failed
     */
    public static void deletePath(String path, BrokerDesc brokerDesc) throws UserException {
        TBrokerDeletePathRequest tDeletePathRequest = new TBrokerDeletePathRequest(
                    TBrokerVersion.VERSION_ONE, path, brokerDesc.getProperties());
        hdfsService.deletePath(tDeletePathRequest);    
    }

    public static boolean checkPathExist(String remotePath, BrokerDesc brokerDesc) throws UserException {
        TBrokerCheckPathExistRequest tCheckPathExistRequest = new TBrokerCheckPathExistRequest(
                TBrokerVersion.VERSION_ONE,
                remotePath, brokerDesc.getProperties());
        return hdfsService.checkPathExist(tCheckPathExistRequest);
    }

    public static void rename(String origFilePath, String destFilePath, BrokerDesc brokerDesc) throws UserException {
        rename(origFilePath, destFilePath, brokerDesc, ClientPool.brokerTimeoutMs);
    }

    public static void rename(String origFilePath, String destFilePath, BrokerDesc brokerDesc, int timeoutMs)
        throws UserException {
        TBrokerRenamePathRequest tRenamePathRequest = new TBrokerRenamePathRequest(TBrokerVersion.VERSION_ONE,
                origFilePath,
                destFilePath, brokerDesc.getProperties());
        hdfsService.renamePath(tRenamePathRequest);
    }

    public static class HdfsReader {
        private String filePath;
        private BrokerDesc brokerDesc;
        private TBrokerFD fd;
        private long currentOffset;
        private boolean isReady;
        private long fileSize;

        public HdfsReader(String filePath, BrokerDesc brokerDesc) {
            this.filePath = filePath;
            this.brokerDesc = brokerDesc;
            this.isReady = false;
        }

        public void open() throws UserException {
            // get file size
            TBrokerListPathRequest request = new TBrokerListPathRequest(TBrokerVersion.VERSION_ONE, filePath, 
                    false, brokerDesc.getProperties());
            List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
            hdfsService.listPath(request, fileStatuses, true, false);
            if (fileStatuses.size() != 1) {
                throw new UserException("HDFS files num error. path=" + filePath + ", files num: " + fileStatuses.size());
            }
            Preconditions.checkState(!fileStatuses.get(0).isIsDir());
            fileSize = fileStatuses.get(0).getSize();

            // open reader
            TBrokerOpenReaderRequest tOpenReaderRequest = new TBrokerOpenReaderRequest(
                    TBrokerVersion.VERSION_ONE, filePath, 0, "", brokerDesc.getProperties());
            fd = hdfsService.openReader(tOpenReaderRequest);
            currentOffset = 0L;
            isReady = true;
        }

        public long getFileSize() {
            return fileSize;
        }

        public byte[] read(long readSize) throws UserException {
            if (!isReady) {
                throw new UserException(
                        "HDFS reader is not ready. filePath=" + filePath);
            }

            readSize = Math.min(readSize, fileSize - currentOffset);
            // read
            TBrokerPReadRequest tPReadRequest = new TBrokerPReadRequest(
                    TBrokerVersion.VERSION_ONE, fd, currentOffset, readSize);
            byte[] result = hdfsService.pread(tPReadRequest);
            currentOffset += result.length;
            return result;
        }

        public void close() {
            // close reader
            if (fd != null) {
                TBrokerCloseReaderRequest tCloseReaderRequest = new TBrokerCloseReaderRequest(
                        TBrokerVersion.VERSION_ONE, fd);
                try {
                    hdfsService.closeReader(tCloseReaderRequest);
                } catch (UserException e) {
                    LOG.warn("HDFS close reader failed. path={}", filePath);
                }
            }
            isReady = false;
        }
    }

    public static class HdfsWriter {
        private String filePath;
        private BrokerDesc brokerDesc;
        private TBrokerFD fd;
        private long currentOffset;
        private boolean isReady;

        public HdfsWriter(String filePath, BrokerDesc brokerDesc) {
            this.filePath = filePath;
            this.brokerDesc = brokerDesc;
            this.isReady = false;
        }

        public void open() throws UserException {
            TBrokerOpenWriterRequest tOpenWriterRequest = new TBrokerOpenWriterRequest(
                    TBrokerVersion.VERSION_ONE, filePath, TBrokerOpenMode.APPEND,
                    "", brokerDesc.getProperties());
            fd = hdfsService.openWriter(tOpenWriterRequest);
            currentOffset = 0L;
            isReady = true;
        }

        public void write(ByteBuffer byteBuffer, long bufferSize) throws UserException {
            if (!isReady) {
                throw new UserException(
                        "HDFS writer is not ready. filePath=" + filePath);
            }

            TBrokerPWriteRequest tPWriteRequest = new TBrokerPWriteRequest(
                    TBrokerVersion.VERSION_ONE, fd, currentOffset, byteBuffer);
            hdfsService.pwrite(tPWriteRequest);
            currentOffset += bufferSize;
        }

        public void close() {
            // close writer
            if (fd != null) {
                TBrokerCloseWriterRequest tCloseWriterRequest = new TBrokerCloseWriterRequest(
                        TBrokerVersion.VERSION_ONE, fd);
                try {
                    hdfsService.closeWriter(tCloseWriterRequest);
                } catch (UserException e) {
                    LOG.warn("HDFS close writer failed. filePath={}", filePath);
                }
            }
            isReady = false;
        }
    }
}
