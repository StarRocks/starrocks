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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/backup/Repository.java

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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.backup.Status.ErrCode;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.List;

/*
 * Repository represents a remote storage for backup to or restore from
 * File organization in repository is:
 *
 * * __starrocks_repository_repo_name/
 *   * __repo_info
 *   * __ss_my_ss1/
 *     * __meta__DJdwnfiu92n
 *     * __info_2018-01-01-08-00-00.OWdn90ndwpu
 *     * __info_2018-01-02-08-00-00.Dnvdio298da
 *     * __info_2018-01-03-08-00-00.O79adbneJWk
 *     * __ss_content/
 *       * __db_10001/
 *         * __tbl_10010/
 *         * __tbl_10020/
 *           * __part_10021/
 *           * __part_10031/
 *             * __idx_10041/
 *             * __idx_10020/
 *               * __10022/
 *               * __10023/
 *                 * __10023_seg1.dat.NUlniklnwDN67
 *                 * __10023_seg2.dat.DNW231dnklawd
 *                 * __10023.hdr.dnmwDDWI92dDko
 */
public class Repository implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(Repository.class);

    public String prefixRepo = "__starrocks_repository_";
    public static final String PREFIX_SNAPSHOT_DIR = "__ss_";
    public static final String PREFIX_DB = "__db_";
    public static final String PREFIX_TBL = "__tbl_";
    public static final String PREFIX_PART = "__part_";
    public static final String PREFIX_IDX = "__idx_";
    public static final String PREFIX_COMMON = "__";
    public static final String PREFIX_JOB_INFO = "__info_";

    public static final String SUFFIX_TMP_FILE = "part";

    public static final String FILE_REPO_INFO = "__repo_info";
    public static final String FILE_META_INFO = "__meta";

    public static final String DIR_SNAPSHOT_CONTENT = "__ss_content";

    private static final String PATH_DELIMITER = "/";
    private static final String CHECKSUM_SEPARATOR = ".";

    @SerializedName("id")
    private long id;
    @SerializedName("nm")
    private String name;
    private String errMsg;
    @SerializedName("ct")
    private long createTime;

    // If True, user can not backup data to this repo.
    @SerializedName("ro")
    private boolean isReadOnly;

    // BOS location should start with "bos://your_bucket_name/"
    // and the specified bucket should exist.
    @SerializedName("lc")
    private String location;

    @SerializedName("st")
    private BlobStorage storage;

    private Repository() {
        // for persist
    }

    public Repository(long id, String name, boolean isReadOnly, String location, BlobStorage storage) {
        this.id = id;
        this.name = name;
        this.isReadOnly = isReadOnly;
        this.location = location;
        this.storage = storage;
        this.createTime = System.currentTimeMillis();
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public String getLocation() {
        return location;
    }

    public String getErrorMsg() {
        return errMsg;
    }

    public BlobStorage getStorage() {
        return storage;
    }

    public long getCreateTime() {
        return createTime;
    }

    // create repository dir and repo info file
    public Status initRepository() {
        Status st = initRepositoryInternal("__palo_repository_");
        if (st.ok()) {
            return Status.OK;
        }
        return initRepositoryInternal("__starrocks_repository_");
    }

    public Status initRepositoryInternal(String path) {
        prefixRepo = path;
        String repoInfoFilePath = assembleRepoInfoFilePath();
        // check if the repo is already exist in remote
        List<RemoteFile> remoteFiles = Lists.newArrayList();
        Status st = storage.list(repoInfoFilePath, remoteFiles);
        if (!st.ok()) {
            return st;
        }
        if (remoteFiles.size() == 1) {
            RemoteFile remoteFile = remoteFiles.get(0);
            if (!remoteFile.isFile()) {
                return new Status(ErrCode.COMMON_ERROR, "the existing repo info is not a file");
            }

            // exist, download and parse the repo info file
            String localFilePath = BackupHandler.BACKUP_ROOT_DIR + "/tmp_info_" + System.currentTimeMillis();
            try {
                st = storage.downloadWithFileSize(repoInfoFilePath, localFilePath, remoteFile.getSize());
                if (!st.ok()) {
                    return st;
                }

                byte[] bytes = Files.readAllBytes(Paths.get(localFilePath));
                String json = new String(bytes, StandardCharsets.UTF_8);
                JSONObject root = new JSONObject(json);
                name = (String) root.get("name");
                createTime = TimeUtils.timeStringToLong((String) root.get("create_time"));
                if (createTime == -1) {
                    return new Status(ErrCode.COMMON_ERROR,
                            "failed to parse create time of repository: " + root.get("create_time"));
                }
                return Status.OK;

            } catch (IOException e) {
                return new Status(ErrCode.COMMON_ERROR, "failed to read repo info file: " + e.getMessage());
            } finally {
                File localFile = new File(localFilePath);
                if (!localFile.delete()) {
                    LOG.warn("Failed to delete file, filepath={}", localFile.getAbsolutePath());
                }
            }

        } else if (remoteFiles.size() > 1) {
            return new Status(ErrCode.COMMON_ERROR,
                    "Invalid repository dir. expected one repo info file. get more: " + remoteFiles);
        } else {
            if (path == "__palo_repository_") {
                return new Status(ErrCode.COMMON_ERROR, "Use new repository prefix");
            }
            // repo is not exist, get repo info
            JSONObject root = new JSONObject();
            root.put("name", name);
            root.put("create_time", TimeUtils.longToTimeString(createTime));
            String repoInfoContent = root.toString();
            return storage.directUpload(repoInfoContent, repoInfoFilePath);
        }
    }

    // eg: location/__starrocks_repository_repo_name/__repo_info
    public String assembleRepoInfoFilePath() {
        return Joiner.on(PATH_DELIMITER).join(location,
                joinPrefix(prefixRepo, name),
                FILE_REPO_INFO);
    }

    // eg: location/__starrocks_repository_repo_name/__my_sp1/__meta
    public String assembleMetaInfoFilePath(String label) {
        return Joiner.on(PATH_DELIMITER).join(location, joinPrefix(prefixRepo, name),
                joinPrefix(PREFIX_SNAPSHOT_DIR, label),
                FILE_META_INFO);
    }

    // eg: location/__starrocks_repository_repo_name/__my_sp1/__info_2018-01-01-08-00-00
    public String assembleJobInfoFilePath(String label, long createTime) {
        return Joiner.on(PATH_DELIMITER).join(location, joinPrefix(prefixRepo, name),
                joinPrefix(PREFIX_SNAPSHOT_DIR, label),
                jobInfoFileNameWithTimestamp(createTime));
    }

    // eg:
    // __starrocks_repository_repo_name/__ss_my_ss1/__ss_content/__db_10001/__tbl_10020/__part_10031/__idx_10020/__10022/
    public String getRepoTabletPathBySnapshotInfo(String label, SnapshotInfo info) {
        return Joiner.on(PATH_DELIMITER).join(location, joinPrefix(prefixRepo, name),
                joinPrefix(PREFIX_SNAPSHOT_DIR, label),
                DIR_SNAPSHOT_CONTENT,
                joinPrefix(PREFIX_DB, info.getDbId()),
                joinPrefix(PREFIX_TBL, info.getTblId()),
                joinPrefix(PREFIX_PART, info.getPartitionId()),
                joinPrefix(PREFIX_IDX, info.getIndexId()),
                joinPrefix(PREFIX_COMMON, info.getTabletId()));
    }

    public String getRepoPath(String label, String childPath) {
        return Joiner.on(PATH_DELIMITER).join(location, joinPrefix(prefixRepo, name),
                joinPrefix(PREFIX_SNAPSHOT_DIR, label),
                DIR_SNAPSHOT_CONTENT,
                childPath);
    }

    // Check if this repo is available.
    // If failed to connect this repo, set errMsg and return false.
    public boolean ping() {
        String checkPath = Joiner.on(PATH_DELIMITER).join(location,
                joinPrefix(prefixRepo, name));
        Status st = storage.checkPathExist(checkPath);
        if (!st.ok()) {
            errMsg = TimeUtils.longToTimeString(System.currentTimeMillis()) + ": " + st.getErrMsg();
            return false;
        }

        // clear err msg
        errMsg = null;

        return true;
    }

    // Visit the repository, and list all existing snapshot names
    public Status listSnapshots(List<String> snapshotNames) {
        // list with prefix:
        // eg. __starrocks_repository_repo_name/__ss_*
        String listPath = Joiner.on(PATH_DELIMITER).join(location, joinPrefix(prefixRepo, name), PREFIX_SNAPSHOT_DIR)
                + "*";
        List<RemoteFile> result = Lists.newArrayList();
        Status st = storage.list(listPath, result);
        if (!st.ok()) {
            return st;
        }

        for (RemoteFile remoteFile : result) {
            if (remoteFile.isFile()) {
                LOG.debug("get snapshot path{} which is not a dir", remoteFile);
                continue;
            }

            snapshotNames.add(disjoinPrefix(PREFIX_SNAPSHOT_DIR, remoteFile.getName()));
        }
        return Status.OK;
    }

    //
    public boolean prepareSnapshotInfo() {
        return false;
    }

    // create remote tablet snapshot path
    // eg:
    // /location/__starrocks_repository_repo_name/__ss_my_ss1/__ss_content/__db_10001/__tbl_10020/__part_10031/__idx_10032/__10023/__3481721
    public String assembleRemoteSnapshotPath(String label, SnapshotInfo info) {
        String path = Joiner.on(PATH_DELIMITER).join(location,
                joinPrefix(prefixRepo, name),
                joinPrefix(PREFIX_SNAPSHOT_DIR, label),
                DIR_SNAPSHOT_CONTENT,
                joinPrefix(PREFIX_DB, info.getDbId()),
                joinPrefix(PREFIX_TBL, info.getTblId()),
                joinPrefix(PREFIX_PART, info.getPartitionId()),
                joinPrefix(PREFIX_IDX, info.getIndexId()),
                joinPrefix(PREFIX_COMMON, info.getTabletId()),
                joinPrefix(PREFIX_COMMON, info.getSchemaHash()));
        LOG.debug("get remote tablet snapshot path: {}", path);
        return path;
    }

    public Status getSnapshotInfoFile(String label, String backupTimestamp, List<BackupJobInfo> infos) {
        String remoteInfoFilePath = assembleJobInfoFilePath(label, -1) + backupTimestamp;
        File localInfoFile = new File(BackupHandler.BACKUP_ROOT_DIR + PATH_DELIMITER
                + "info_" + System.currentTimeMillis());
        try {
            Status st = download(remoteInfoFilePath, localInfoFile.getPath());
            if (!st.ok()) {
                return st;
            }

            BackupJobInfo jobInfo = BackupJobInfo.fromFile(localInfoFile.getAbsolutePath());
            infos.add(jobInfo);
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Failed to create job info from file: "
                    + "" + localInfoFile.getName() + ". msg: " + e.getMessage());
        } finally {
            if (!localInfoFile.delete()) {
                LOG.warn("Failed to delete file, filepath={}", localInfoFile.getAbsolutePath());
            }
        }

        return Status.OK;
    }

    public Status getSnapshotMetaFile(String label, List<BackupMeta> backupMetas, int metaVersion,
                                      int starrocksMetaVersion) {
        String remoteMetaFilePath = assembleMetaInfoFilePath(label);
        File localMetaFile = new File(BackupHandler.BACKUP_ROOT_DIR + PATH_DELIMITER
                + "meta_" + System.currentTimeMillis());

        try {
            Status st = download(remoteMetaFilePath, localMetaFile.getAbsolutePath());
            if (!st.ok()) {
                return st;
            }

            // read file to backupMeta
            BackupMeta backupMeta =
                    BackupMeta.fromFile(localMetaFile.getAbsolutePath(), starrocksMetaVersion);
            backupMetas.add(backupMeta);
        } catch (IOException e) {
            LOG.warn("failed to read backup meta from file", e);
            return new Status(ErrCode.COMMON_ERROR, "Failed create backup meta from file: "
                    + localMetaFile.getAbsolutePath() + ", msg: " + e.getMessage());
        } finally {
            if (!localMetaFile.delete()) {
                LOG.warn("Failed to delete file, filepath={}", localMetaFile.getAbsolutePath());
            }
        }

        return Status.OK;
    }

    // upload the local file to specified remote file with checksum
    // remoteFilePath should be FULL path
    public Status upload(String localFilePath, String remoteFilePath) {
        // Preconditions.checkArgument(remoteFilePath.startsWith(location), remoteFilePath);
        // get md5usm of local file
        File file = new File(localFilePath);
        String md5sum;
        try {
            md5sum = DigestUtils.md5Hex(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            return new Status(ErrCode.NOT_FOUND, "file " + localFilePath + " does not exist");
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "failed to get md5sum of file: " + localFilePath);
        }
        Preconditions.checkState(!Strings.isNullOrEmpty(md5sum));
        String tmpRemotePath = assembleFileNameWithSuffix(remoteFilePath, SUFFIX_TMP_FILE);
        String finalRemotePath = assembleFileNameWithSuffix(remoteFilePath, md5sum);
        LOG.debug("get md5sum of file: {}. tmp remote path: {}. final remote path: {}", localFilePath, tmpRemotePath,
                finalRemotePath);

        // this may be a retry, so we should first delete remote file
        Status st = storage.delete(tmpRemotePath);
        if (!st.ok()) {
            return st;
        }

        st = storage.delete(finalRemotePath);
        if (!st.ok()) {
            return st;
        }

        // upload tmp file
        st = storage.upload(localFilePath, tmpRemotePath);
        if (!st.ok()) {
            return st;
        }

        // rename tmp file with checksum named file
        st = storage.rename(tmpRemotePath, finalRemotePath);
        if (!st.ok()) {
            return st;
        }
        LOG.info("finished to upload local file {} to remote file: {}", localFilePath, finalRemotePath);
        return st;
    }

    // remoteFilePath must be a file(not dir) and does not contain checksum
    public Status download(String remoteFilePath, String localFilePath) {
        // 0. list to get to full name(with checksum)
        List<RemoteFile> remoteFiles = Lists.newArrayList();
        Status status = storage.list(remoteFilePath + "*", remoteFiles);
        if (!status.ok()) {
            return status;
        }
        if (remoteFiles.size() != 1) {
            return new Status(ErrCode.COMMON_ERROR,
                    "Expected one file with path: " + remoteFilePath + ". get: " + remoteFiles.size());
        }
        if (!remoteFiles.get(0).isFile()) {
            return new Status(ErrCode.COMMON_ERROR, "Expected file with path: " + remoteFilePath + ". but get dir");
        }

        String remoteFilePathWithChecksum = replaceFileNameWithChecksumFileName(remoteFilePath,
                remoteFiles.get(0).getName());
        LOG.debug("get download filename with checksum: " + remoteFilePathWithChecksum);

        // 1. get checksum from remote file name
        Pair<String, String> pair = decodeFileNameWithChecksum(remoteFilePathWithChecksum);
        if (pair == null) {
            return new Status(ErrCode.COMMON_ERROR,
                    "file name should contains checksum: " + remoteFilePathWithChecksum);
        }
        if (!remoteFilePath.endsWith(pair.first)) {
            return new Status(ErrCode.COMMON_ERROR, "File does not exist: " + remoteFilePath);
        }
        String md5sum = pair.second;

        // 2. download
        status = storage.downloadWithFileSize(remoteFilePathWithChecksum, localFilePath, remoteFiles.get(0).getSize());
        if (!status.ok()) {
            return status;
        }

        // 3. verify checksum
        String localMd5sum;
        try {
            localMd5sum = DigestUtils.md5Hex(new FileInputStream(localFilePath));
        } catch (FileNotFoundException e) {
            return new Status(ErrCode.NOT_FOUND, "file " + localFilePath + " does not exist");
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "failed to get md5sum of file: " + localFilePath);
        }

        if (!localMd5sum.equals(md5sum)) {
            return new Status(ErrCode.BAD_FILE,
                    "md5sum does not equal. local: " + localMd5sum + ", remote: " + md5sum);
        }

        return Status.OK;
    }

    // join job info file name with timestamp
    // eg: __info_2018-01-01-08-00-00
    private static String jobInfoFileNameWithTimestamp(long createTime) {
        if (createTime == -1) {
            return PREFIX_JOB_INFO;
        } else {
            return PREFIX_JOB_INFO
                    + TimeUtils.longToTimeString(createTime, new SimpleDateFormat(BackupJob.TIMESTAMP_FORMAT));
        }
    }

    // join the name with specified prefix
    private static String joinPrefix(String prefix, Object name) {
        return prefix + name;
    }

    // disjoint the name with specified prefix
    private static String disjoinPrefix(String prefix, String nameWithPrefix) {
        return nameWithPrefix.substring(prefix.length());
    }

    private static String assembleFileNameWithSuffix(String filePath, String md5sum) {
        return filePath + CHECKSUM_SEPARATOR + md5sum;
    }

    public static Pair<String, String> decodeFileNameWithChecksum(String fileNameWithChecksum) {
        int index = fileNameWithChecksum.lastIndexOf(CHECKSUM_SEPARATOR);
        if (index == -1) {
            return null;
        }
        String fileName = fileNameWithChecksum.substring(0, index);
        String md5sum = fileNameWithChecksum.substring(index + CHECKSUM_SEPARATOR.length());

        if (md5sum.length() != 32) {
            return null;
        }

        return Pair.create(fileName, md5sum);
    }

    // in: /path/to/orig_file
    // out: /path/to/orig_file.BUWDnl831e4nldsf
    public static String replaceFileNameWithChecksumFileName(String origPath, String fileNameWithChecksum) {
        return origPath.substring(0, origPath.lastIndexOf(PATH_DELIMITER) + 1) + fileNameWithChecksum;
    }

    public Status getBrokerAddress(Long beId, GlobalStateMgr globalStateMgr, List<FsBroker> brokerAddrs) {
        // get backend
        Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(beId);
        if (be == null) {
            return new Status(ErrCode.COMMON_ERROR, "backend " + beId + " is missing. "
                    + "failed to send upload snapshot task");
        }

        // get proper broker for this backend
        FsBroker brokerAddr;
        try {
            brokerAddr = globalStateMgr.getBrokerMgr().getBroker(storage.getBrokerName(), be.getHost());
        } catch (AnalysisException e) {
            return new Status(ErrCode.COMMON_ERROR, "failed to get address of broker "
                    + storage.getBrokerName() + " when try to send upload snapshot task: "
                    + e.getMessage());
        }
        if (brokerAddr == null) {
            return new Status(ErrCode.COMMON_ERROR, "failed to get address of broker "
                    + storage.getBrokerName() + " when try to send upload snapshot task");
        }
        brokerAddrs.add(brokerAddr);
        return Status.OK;
    }

    public List<String> getInfo() {
        List<String> info = Lists.newArrayList();
        info.add(String.valueOf(id));
        info.add(name);
        info.add(TimeUtils.longToTimeString(createTime));
        info.add(String.valueOf(isReadOnly));
        info.add(location);
        info.add(storage.getBrokerName());
        info.add(errMsg == null ? FeConstants.NULL_STRING : errMsg);
        return info;
    }

    public List<List<String>> getSnapshotInfos(String snapshotName, String timestamp, List<String> snapshotNames)
            throws AnalysisException {
        List<List<String>> snapshotInfos = Lists.newArrayList();
        if (Strings.isNullOrEmpty(snapshotName)) {
            // get all snapshot infos
            List<String> fullSnapshotNames = Lists.newArrayList();
            Status status = listSnapshots(fullSnapshotNames);
            if (!status.ok()) {
                throw new AnalysisException(
                        "Failed to list snapshot in repo: " + name + ", err: " + status.getErrMsg());
            }

            for (String ssName : fullSnapshotNames) {
                if (snapshotNames != null && snapshotNames.size() != 0 && !snapshotNames.contains(ssName)) {
                    continue;
                }
                List<String> info = getSnapshotInfo(ssName, null /* get all timestamp */);
                snapshotInfos.add(info);
            }
        } else {
            // get specified snapshot info
            List<String> info = getSnapshotInfo(snapshotName, timestamp);
            snapshotInfos.add(info);
        }

        return snapshotInfos;
    }

    private List<String> getSnapshotInfo(String snapshotName, String timestamp) {
        List<String> info = Lists.newArrayList();
        if (Strings.isNullOrEmpty(timestamp)) {
            // get all timestamp
            // path eg: /location/__starrocks_repository_repo_name/__ss_my_snap/__info_*
            String infoFilePath = assembleJobInfoFilePath(snapshotName, -1);
            LOG.debug("assemble infoFilePath: {}, snapshot: {}", infoFilePath, snapshotName);
            List<RemoteFile> results = Lists.newArrayList();
            Status st = storage.list(infoFilePath + "*", results);
            if (!st.ok()) {
                info.add(snapshotName);
                info.add(FeConstants.NULL_STRING);
                info.add("ERROR: Failed to get info: " + st.getErrMsg());
            } else {
                info.add(snapshotName);

                List<String> tmp = Lists.newArrayList();
                for (RemoteFile file : results) {
                    // __info_2018-04-18-20-11-00.Jdwnd9312sfdn1294343
                    Pair<String, String> pureFileName = decodeFileNameWithChecksum(file.getName());
                    if (pureFileName == null) {
                        // maybe: __info_2018-04-18-20-11-00.part
                        tmp.add("Invalid: " + file.getName());
                        continue;
                    }
                    tmp.add(disjoinPrefix(PREFIX_JOB_INFO, pureFileName.first));
                }
                info.add(Joiner.on("\n").join(tmp));
                info.add(tmp.isEmpty() ? "ERROR: no snapshot" : "OK");
            }
        } else {
            // get specified timestamp
            // path eg: /path/to/backup/__info_2081-04-19-12-59-11
            String localFilePath = BackupHandler.BACKUP_ROOT_DIR + "/" + Repository.PREFIX_JOB_INFO + timestamp;
            try {
                String remoteInfoFilePath = assembleJobInfoFilePath(snapshotName, -1) + timestamp;
                Status st = download(remoteInfoFilePath, localFilePath);
                if (!st.ok()) {
                    info.add(snapshotName);
                    info.add(timestamp);
                    info.add(FeConstants.NULL_STRING);
                    info.add(FeConstants.NULL_STRING);
                    info.add("Failed to get info: " + st.getErrMsg());
                } else {
                    try {
                        BackupJobInfo jobInfo = BackupJobInfo.fromFile(localFilePath);
                        info.add(snapshotName);
                        info.add(timestamp);
                        info.add(jobInfo.dbName);
                        info.add(jobInfo.getBrief());
                        info.add("OK");
                    } catch (IOException e) {
                        info.add(snapshotName);
                        info.add(timestamp);
                        info.add(FeConstants.NULL_STRING);
                        info.add(FeConstants.NULL_STRING);
                        info.add("Failed to read info from local file: " + e.getMessage());
                    }
                }
            } finally {
                // delete tmp local file
                File localFile = new File(localFilePath);
                if (localFile.exists()) {
                    if (!localFile.delete()) {
                        LOG.warn("Failed to delete file, filepath={}", localFile.getAbsolutePath());
                    }
                }
            }
        }

        return info;
    }

    public static Repository read(DataInput in) throws IOException {
        Repository repo = new Repository();
        repo.readFields(in);
        return repo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        Text.writeString(out, name);
        out.writeBoolean(isReadOnly);
        Text.writeString(out, location);
        storage.write(out);
        out.writeLong(createTime);
    }

    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        name = Text.readString(in);
        isReadOnly = in.readBoolean();
        location = Text.readString(in);
        storage = BlobStorage.read(in);
        createTime = in.readLong();

        if (!GlobalStateMgr.isCheckpointThread()) {
            genPrefixRepo();
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (!GlobalStateMgr.isCheckpointThread()) {
            genPrefixRepo();
        }
    }

    private void genPrefixRepo() {
        // check __palo_repository_ first, if success, prefixRepo = __palo_repository_
        String listPath = Joiner.on(PATH_DELIMITER).join(location, joinPrefix("__palo_repository_", name));
        Status st;
        try {
            st = storage.checkPathExist(listPath);
        } catch (Exception e) {
            LOG.warn("check path exist fail");
            prefixRepo = "__starrocks_repository_";
            return;
        }

        if (st.ok()) {
            prefixRepo = "__palo_repository_";
        } else {
            prefixRepo = "__starrocks_repository_";
        }
    }
}
