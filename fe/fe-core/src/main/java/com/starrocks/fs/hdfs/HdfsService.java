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

<<<<<<< HEAD
import com.starrocks.common.UserException;
=======
import com.starrocks.common.StarRocksException;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.thrift.TBrokerCheckPathExistRequest;
import com.starrocks.thrift.TBrokerCloseReaderRequest;
import com.starrocks.thrift.TBrokerCloseWriterRequest;
import com.starrocks.thrift.TBrokerDeletePathRequest;
import com.starrocks.thrift.TBrokerFD;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TBrokerListPathRequest;
import com.starrocks.thrift.TBrokerOpenReaderRequest;
import com.starrocks.thrift.TBrokerOpenWriterRequest;
import com.starrocks.thrift.TBrokerPReadRequest;
import com.starrocks.thrift.TBrokerPWriteRequest;
import com.starrocks.thrift.TBrokerRenamePathRequest;
import com.starrocks.thrift.TBrokerSeekRequest;
import com.starrocks.thrift.THdfsProperties;
<<<<<<< HEAD
=======
import org.apache.hadoop.fs.FileStatus;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;


public class HdfsService {

    private static Logger LOG = LogManager.getLogger(HdfsService.class);
    private HdfsFsManager fileSystemManager;

    public HdfsService() {
        fileSystemManager = new HdfsFsManager();
    }

    public void getTProperties(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
<<<<<<< HEAD
            throws UserException {
=======
            throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        fileSystemManager.getTProperties(path, loadProperties, tProperties);
    }

    public void listPath(TBrokerListPathRequest request, List<TBrokerFileStatus> fileStatuses, boolean skipDir,
<<<<<<< HEAD
                         boolean fileNameOnly) throws UserException {
=======
                         boolean fileNameOnly) throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        LOG.info("receive a list path request, path: {}", request.path);
        List<TBrokerFileStatus> allFileStatuses = fileSystemManager.listPath(request.path, fileNameOnly,
                request.properties);

        for (TBrokerFileStatus tBrokerFileStatus : allFileStatuses) {
            if (skipDir && tBrokerFileStatus.isDir) {
                continue;
            }
            fileStatuses.add(tBrokerFileStatus);
        }
    }

<<<<<<< HEAD
    public void deletePath(TBrokerDeletePathRequest request)
            throws UserException {
=======
    public List<FileStatus> listFileMeta(String path, Map<String, String> properties, boolean skipDir)
            throws StarRocksException {
        LOG.info("try to list file meta: {}", path);
        List<FileStatus> allFileStatuses = fileSystemManager.listFileMeta(path, properties);
        if (skipDir) {
            allFileStatuses.removeIf(FileStatus::isDirectory);
        }
        return allFileStatuses;
    }

    public void deletePath(TBrokerDeletePathRequest request)
            throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        LOG.info("receive a delete path request, path: {}", request.path);
        fileSystemManager.deletePath(request.path, request.properties);
    }

    public void renamePath(TBrokerRenamePathRequest request)
<<<<<<< HEAD
            throws UserException {
=======
            throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        LOG.info("receive a rename path request, source path: {}, dest path: {}",
                request.srcPath, request.destPath);
        fileSystemManager.renamePath(request.srcPath, request.destPath, request.properties);
    }

    public boolean checkPathExist(
<<<<<<< HEAD
            TBrokerCheckPathExistRequest request) throws UserException {
=======
            TBrokerCheckPathExistRequest request) throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        LOG.info("receive a check path request, path: {}", request.path);
        return fileSystemManager.checkPathExist(request.path, request.properties);
    }

    public TBrokerFD openReader(TBrokerOpenReaderRequest request)
<<<<<<< HEAD
            throws UserException {
=======
            throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        LOG.info("receive a open reader request, path: {}, start offset: {}, client id: {}",
                request.path, request.startOffset, request.clientId);
        return fileSystemManager.openReader(request.path,
                request.startOffset, request.properties);
    }

    public byte[] pread(TBrokerPReadRequest request)
<<<<<<< HEAD
            throws UserException {
=======
            throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        LOG.debug("receive a read request, fd: {}, offset: {}, length: {}",
                request.fd, request.offset, request.length);
        return fileSystemManager.pread(request.fd, request.offset, request.length);
    }

    public void seek(TBrokerSeekRequest request)
<<<<<<< HEAD
            throws UserException {
=======
            throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        LOG.debug("receive a seek request, fd: {}, offset: {}", request.fd, request.offset);
        fileSystemManager.seek(request.fd, request.offset);
    }

    public void closeReader(TBrokerCloseReaderRequest request)
<<<<<<< HEAD
            throws UserException {
=======
            throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        LOG.info("receive a close reader request, fd: {}", request.fd);
        fileSystemManager.closeReader(request.fd);
    }

    public TBrokerFD openWriter(TBrokerOpenWriterRequest request)
<<<<<<< HEAD
            throws UserException {
=======
            throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        LOG.info("receive a open writer request, path: {}, mode: {}, client id: {}",
                request.path, request.openMode, request.clientId);
        TBrokerFD fd = fileSystemManager.openWriter(request.path, request.properties);
        return fd;
    }

    public void pwrite(TBrokerPWriteRequest request)
<<<<<<< HEAD
            throws UserException {
=======
            throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        LOG.debug("receive a pwrite request, fd: {}, offset: {}, size: {}",
                request.fd, request.offset, request.data.remaining());
        fileSystemManager.pwrite(request.fd, request.offset, request.getData());
    }

    public void closeWriter(TBrokerCloseWriterRequest request)
<<<<<<< HEAD
            throws UserException {
=======
            throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        LOG.info("receive a close writer request, fd: {}", request.fd);
        fileSystemManager.closeWriter(request.fd);
    }
}
