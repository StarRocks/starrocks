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


package com.staros.filestore;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.staros.credential.ADLS2Credential;
import com.staros.credential.AwsCredential;
import com.staros.credential.AwsCredentialMgr;
import com.staros.credential.AzBlobCredential;
import com.staros.credential.GSCredential;
import com.staros.exception.AlreadyExistsStarException;
import com.staros.exception.ExceptionCode;
import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.NotExistStarException;
import com.staros.exception.StarException;
import com.staros.journal.DummyJournalSystem;
import com.staros.journal.Journal;
import com.staros.journal.JournalSystem;
import com.staros.journal.StarMgrJournal;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreMgrImageMetaFooter;
import com.staros.proto.FileStoreMgrImageMetaHeader;
import com.staros.proto.FileStoreType;
import com.staros.proto.SectionType;
import com.staros.section.Section;
import com.staros.section.SectionReader;
import com.staros.section.SectionWriter;
import com.staros.util.Config;
import com.staros.util.Constant;
import com.staros.util.LockCloseable;
import com.staros.util.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.staros.util.Utils.executeNoExceptionOrDie;

public class FileStoreMgr {
    private static final Logger LOG = LogManager.getLogger(FileStoreMgr.class);

    private String serviceId;

    // File store unit key to FileStoreUnit object
    private Map<String, FileStore> fileStores;

    private ReentrantReadWriteLock lock;

    private JournalSystem journalSystem;

    // FOR TEST
    public static FileStoreMgr createFileStoreMgrForTest(String serviceId) {
        return new FileStoreMgr(serviceId, new DummyJournalSystem());
    }

    public FileStoreMgr(String serviceId, JournalSystem journalSystem) {
        this.serviceId = serviceId;
        this.fileStores = new HashMap<String, FileStore>();
        this.lock = new ReentrantReadWriteLock();
        this.journalSystem = journalSystem;
        addFileStoresFromConfig();
    }

    public FileStore getFileStore(String fsKey) {
        try (LockCloseable lockCloseable = new LockCloseable(lock.readLock())) {
            return fileStores.get(fsKey);
        }
    }

    public void addFileStore(FileStore fs) throws StarException {
        addFileStore(fs, false);
    }

    private void addFileStore(FileStore fs, boolean isReplay) throws StarException {
        if (!fs.isValid()) {
            throw new InvalidArgumentStarException("Invalid file store (fsKey: {}), please check", fs.key());
        }

        try (LockCloseable lockCloseable = new LockCloseable(lock.writeLock())) {
            if (fileStores.containsKey(fs.key())) {
                throw new AlreadyExistsStarException("File store with key {} already exist", fs.key());
            }
            if (!isReplay && !fs.isBuiltin()) {
                Journal journal = StarMgrJournal.logAddFileStore(serviceId, fs.toProtobuf());
                journalSystem.write(journal);
            }
            executeNoExceptionOrDie(() -> fileStores.put(fs.key(), fs));
            LOG.info("Add file store {}", fs);
        }
    }

    public void removeFileStore(String fsKey) throws StarException {
        removeFileStore(fsKey, false);
    }

    private void removeFileStore(String fsKey, boolean isReplay) throws StarException {
        try (LockCloseable lockCloseable = new LockCloseable(lock.writeLock())) {
            if (!fileStores.containsKey(fsKey)) {
                throw new NotExistStarException("File store with key {} not exist", fsKey);
            }

            FileStore fs = fileStores.get(fsKey);
            if (fs.isBuiltin()) {
                throw new InvalidArgumentStarException("Builtin file store can not be removed");
            }
            if (!isReplay) {
                Journal journal = StarMgrJournal.logRemoveFileStore(serviceId, fsKey);
                journalSystem.write(journal);
            }
            executeNoExceptionOrDie(() -> fileStores.remove(fsKey));
            LOG.info("Remove file store {}", fs);
        }
    }

    public void removeFileStoreByName(String fsName) throws StarException {
        removeFileStoreByName(fsName, false);
    }

    private void removeFileStoreByName(String fsName, boolean isReplay) throws StarException {
        try (LockCloseable lockCloseable = new LockCloseable(lock.writeLock())) {
            FileStore fs = getFileStoreByName(fsName);
            if (fs == null) {
                throw new NotExistStarException("File store with name {} not exist", fsName);
            }
            if (fs.isBuiltin()) {
                throw new InvalidArgumentStarException("Builtin file store can not be removed");
            }
            if (!isReplay) {
                Journal journal = StarMgrJournal.logRemoveFileStore(serviceId, fs.key());
                journalSystem.write(journal);
            }
            executeNoExceptionOrDie(() -> fileStores.remove(fs.key()));
            LOG.info("Remove file store {}", fs);
        }
    }

    public void updateFileStore(FileStore fs) throws StarException {
        updateFileStore(fs, false);
    }

    private void updateFileStore(FileStore fs, boolean isReplay) throws StarException {
        try (LockCloseable lockCloseable = new LockCloseable(lock.writeLock())) {
            if (!fileStores.containsKey(fs.key())) {
                throw new NotExistStarException("File store with key {} not exist", fs.key());
            }
            if (fs.isBuiltin()) {
                throw new InvalidArgumentStarException("Builtin file store can not be updated");
            }

            if (!isReplay && !fs.isBuiltin()) {
                Journal journal = StarMgrJournal.logUpdateFileStore(serviceId, fs.toProtobuf());
                journalSystem.write(journal);
            }
            executeNoExceptionOrDie(() -> {
                FileStore oldFs = fileStores.get(fs.key());
                oldFs.mergeFrom(fs);
                oldFs.increaseVersion();
            });
            LOG.info("Update file store {}", fs);
        }
    }

    public void replaceFileStore(FileStore fs) throws StarException {
        replaceFileStore(fs, false);
    }

    private void replaceFileStore(FileStore fs, boolean isReplay) throws StarException {
        try (LockCloseable lockCloseable = new LockCloseable(lock.writeLock())) {
            FileStore oldFs = fileStores.get(fs.key());
            if (oldFs == null) {
                throw new NotExistStarException("File store with key {} not exist", fs.key());
            }
            if (oldFs.isBuiltin()) {
                throw new InvalidArgumentStarException("Builtin file store can not be replaced");
            }

            if (!isReplay) {
                Journal journal = StarMgrJournal.logReplaceFileStore(serviceId, fs.toProtobuf());
                journalSystem.write(journal);
            }

            executeNoExceptionOrDie(() -> {
                fs.setVersion(oldFs.getVersion());
                fs.increaseVersion();
                fileStores.put(fs.key(), fs);
            });
            LOG.info("Replace file store {}", fs);
        }
    }

    public List<FileStore> listFileStore(FileStoreType fsType) throws StarException {
        try (LockCloseable lockCloseable = new LockCloseable(lock.readLock())) {
            if (fsType == Constant.FS_NOT_SET) {
                return new ArrayList<FileStore>(fileStores.values());
            }
            return fileStores.values().stream().filter(fileStore -> fileStore.type() == fsType)
                    .collect(Collectors.toList());
        }
    }

    // Allocate a file store to hold shard data
    public FileStore allocFileStore(String fsKey) throws StarException {
        try (LockCloseable lockCloseable = new LockCloseable(lock.readLock())) {
            if (fileStores.isEmpty()) {
                throw new NotExistStarException("No any file store exist");
            }

            if (fsKey.isEmpty()) {
                // choose the first one now, will add some policy in future
                // TODO: use the default file store first.
                Iterator<FileStore> enabledFileStores = fileStores.values()
                        .stream().filter(fileStore -> fileStore.getEnabled()).iterator();
                if (!enabledFileStores.hasNext()) {
                    throw new NotExistStarException("No any enabled file store exist");
                }
                return enabledFileStores.next();
            }

            if (!fileStores.containsKey(fsKey)) {
                throw new NotExistStarException("file store {} not exists", fsKey);
            }
            FileStore fs = fileStores.get(fsKey);
            if (!fs.getEnabled()) {
                throw new InvalidArgumentStarException("file store {} is disabled", fsKey);
            }
            return fs;
        }
    }

    public void addFileStoresFromConfig() {
        List<FileStore> fileStores = new ArrayList<>();
        fileStores.add(loadS3FileStoreFromConfig());
        fileStores.add(loadHDFSFileStoreFromConfig());
        fileStores.add(loadAzBlobFileStoreFromConfig());
        fileStores.add(loadADLS2FileStoreFromConfig());
        fileStores.add(loadGSFileStoreFromConfig());

        for (FileStore fileStore : fileStores) {
            if (fileStore != null) {
                try {
                    addFileStore(fileStore);
                    LOG.debug("Add hdfs file store with key {} from config", fileStore.key());
                } catch (StarException e) {
                    if (e.getExceptionCode() == ExceptionCode.ALREADY_EXIST) {
                        updateFileStore(fileStore);
                    } else {
                        LOG.info("no default file store configured");
                    }
                }
            }
        }
    }

    public FileStore loadAzBlobFileStoreFromConfig() {
        if (Config.AZURE_BLOB_ENDPOINT.isEmpty()) {
            LOG.info("Empty AZURE_BLOB_ENDPOINT configured, skip load");
            return null;
        }

        if (Config.AZURE_BLOB_PATH.isEmpty()) {
            LOG.info("Empty AZURE_BLOB_PATH configured, skip load");
            return null;
        }

        AzBlobCredential credential = new AzBlobCredential(Config.AZURE_BLOB_SHARED_KEY, Config.AZURE_BLOB_SAS_TOKEN,
                Config.AZURE_BLOB_TENANT_ID, Config.AZURE_BLOB_CLIENT_ID, Config.AZURE_BLOB_CLIENT_SECRET,
                Config.AZURE_BLOB_CLIENT_CERTIFICATE_PATH, Config.AZURE_BLOB_AUTHORITY_HOST);

        AzBlobFileStore azblob = new AzBlobFileStore(Constant.AZURE_BLOB_FSKEY_FOR_CONFIG,
                Constant.AZURE_BLOB_FSNAME_FOR_CONFIG, Config.AZURE_BLOB_ENDPOINT, Config.AZURE_BLOB_PATH, credential);
        azblob.setBuiltin(true);
        return azblob;
    }

    public FileStore loadADLS2FileStoreFromConfig() {
        if (Config.AZURE_ADLS2_ENDPOINT.isEmpty()) {
            LOG.info("Empty AZURE_ADLS2_ENDPOINT configured, skip load");
            return null;
        }

        if (Config.AZURE_ADLS2_PATH.isEmpty()) {
            LOG.info("Empty AZURE_ADLS2_PATH configured, skip load");
            return null;
        }

        ADLS2Credential credential = new ADLS2Credential(Config.AZURE_ADLS2_SHARED_KEY, Config.AZURE_ADLS2_SAS_TOKEN,
                Config.AZURE_ADLS2_TENANT_ID, Config.AZURE_ADLS2_CLIENT_ID, Config.AZURE_ADLS2_CLIENT_SECRET,
                Config.AZURE_ADLS2_CLIENT_CERTIFICATE_PATH, Config.AZURE_ADLS2_AUTHORITY_HOST);

        ADLS2FileStore adls2 = new ADLS2FileStore(Constant.AZURE_ADLS2_FSKEY_FOR_CONFIG,
                Constant.AZURE_ADLS2_FSNAME_FOR_CONFIG, Config.AZURE_ADLS2_ENDPOINT, Config.AZURE_ADLS2_PATH,
                credential);
        adls2.setBuiltin(true);
        return adls2;
    }

    public FileStore loadGSFileStoreFromConfig() {
        GSCredential credential =
                new GSCredential(Config.GCP_GS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT, Config.GCP_GS_SERVICE_ACCOUNT_EMAIL,
                        Config.GCP_GS_SERVICE_ACCOUNT_PRIVATE_KEY_ID, Config.GCP_GS_SERVICE_ACCOUNT_PRIVATE_KEY,
                        Config.GCP_GS_IMPERSONATION);
        GSFileStore gsFileStore = new GSFileStore(Constant.GS_FSKEY_FOR_CONFIG,
                Constant.GS_FSNAME_FOR_CONFIG, Config.GCP_GS_ENDPOINT, Config.GCP_GS_PATH, credential);
        gsFileStore.setBuiltin(true);
        return gsFileStore;
    }

    public FileStore loadHDFSFileStoreFromConfig() {
        HDFSFileStore hdfs = new HDFSFileStore(Constant.HDFS_FSKEY_FOR_CONFIG, Constant.HDFS_FSNAME_FOR_CONFIG,
                Config.HDFS_URL);
        hdfs.setBuiltin(true);
        return hdfs;
    }

    public FileStore loadS3FileStoreFromConfig() {
        AwsCredential credential = AwsCredentialMgr.getCredentialFromConfig();
        if (credential == null) {
            LOG.warn("get credential from config error");
            return null;
        }

        if (Config.S3_BUCKET.isEmpty()) {
            LOG.info("Empty S3_BUCKET configured, skip load");
            return null;
        }
        if (Config.S3_BUCKET.contains("/")) {
            LOG.warn("Invalid S3_BUCKET configuration:{}", Config.S3_BUCKET);
            throw new InvalidArgumentStarException("Invalid S3_BUCKET configuration:{}", Config.S3_BUCKET);
        }

        S3FileStore s3fs = new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG, Config.S3_BUCKET,
                Config.S3_REGION, Config.S3_ENDPOINT, credential, Config.S3_PATH_PREFIX);
        s3fs.setBuiltin(true);
        return s3fs;
    }

    public FileStore getFileStoreByName(String fsName) {
        try (LockCloseable lockCloseable = new LockCloseable(lock.readLock())) {
            for (FileStore fileStore : fileStores.values()) {
                if (fileStore.name().equals(fsName)) {
                    return fileStore;
                }
            }
        }
        return null;
    }

    public void replayAddFileStore(FileStoreInfo fsInfo) {
        addFileStore(FileStore.fromProtobuf(fsInfo), true);
    }

    public void replayRemoveFileStore(String fsKey) {
        removeFileStore(fsKey, true);
    }

    public void replayUpdateFileStore(FileStoreInfo fsInfo) {
        updateFileStore(FileStore.fromProtobuf(fsInfo), true);
    }

    public void replayReplaceFileStore(FileStoreInfo fsInfo) {
        replaceFileStore(FileStore.fromProtobuf(fsInfo), true);
    }

    public void dumpMeta(OutputStream out) throws IOException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            LOG.debug("start dump filestore manager meta data ...");

            // write header
            // TODO: support builtin fs persistence.
            FileStoreMgrImageMetaHeader header = FileStoreMgrImageMetaHeader.newBuilder()
                    .setDigestAlgorithm(Constant.DEFAULT_DIGEST_ALGORITHM)
                    .setNumFileStore((int) fileStores.values().stream().filter(fileStore -> !fileStore.isBuiltin()).count())
                    .build();
            header.writeDelimitedTo(out);

            DigestOutputStream mdStream = Utils.getDigestOutputStream(out, Constant.DEFAULT_DIGEST_ALGORITHM);
            try (SectionWriter writer = new SectionWriter(mdStream)) {
                try (OutputStream stream = writer.appendSection(SectionType.SECTION_FILESTOREMGR_FILESTORE)) {
                    dumpFileStores(stream);
                }
            }
            // flush the mdStream because we are going to write to `out` directly.
            mdStream.flush();

            // write MetaFooter, DO NOT directly write new data here, change the protobuf definition
            FileStoreMgrImageMetaFooter.Builder footerBuilder = FileStoreMgrImageMetaFooter.newBuilder();
            if (mdStream.getMessageDigest() != null) {
                footerBuilder.setChecksum(ByteString.copyFrom(mdStream.getMessageDigest().digest()));
            }
            footerBuilder.build().writeDelimitedTo(out);
            LOG.debug("end dump filestore manager meta data.");
        }
    }

    private void dumpFileStores(OutputStream stream) throws IOException {
        for (FileStore fileStore : fileStores.values()) {
            if (!fileStore.isBuiltin()) {
                fileStore.toProtobuf().writeDelimitedTo(stream);
            }
        }
    }

    public void loadMeta(InputStream in) throws IOException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            LOG.debug("start load file store manager meta data ...");

            // read header
            FileStoreMgrImageMetaHeader header = FileStoreMgrImageMetaHeader.parseDelimitedFrom(in);
            if (header == null) {
                throw new EOFException();
            }
            DigestInputStream digestInput = Utils.getDigestInputStream(in, header.getDigestAlgorithm());

            try (SectionReader reader = new SectionReader(digestInput)) {
                reader.forEach(x -> loadFileStoreMgrSection(x, header));
            }

            // read MetaFooter
            FileStoreMgrImageMetaFooter footer = FileStoreMgrImageMetaFooter.parseDelimitedFrom(in);
            if (footer == null) {
                throw new EOFException();
            }
            Utils.validateChecksum(digestInput.getMessageDigest(), footer.getChecksum());
            LOG.debug("end load file store manager meta data.");
        }
    }

    private void loadFileStoreMgrSection(Section section, FileStoreMgrImageMetaHeader header) throws IOException {
        switch (section.getHeader().getSectionType()) {
            case SECTION_FILESTOREMGR_FILESTORE:
                loadFileStores(section.getStream(), header.getNumFileStore());
                break;
            default:
                LOG.warn("Unknown section type:{} when loadMeta in FileStoreMgr, ignore it!",
                        section.getHeader().getSectionType());
                break;
        }
    }

    private void loadFileStores(InputStream stream, int numFileStore) throws IOException {
        for (int i = 0; i < numFileStore; ++i) {
            FileStoreInfo fsInfo = FileStoreInfo.parseDelimitedFrom(stream);
            if (fsInfo == null) {
                throw new EOFException();
            }

            FileStore fs = FileStore.fromProtobuf(fsInfo);
            if (fs.type() == FileStoreType.INVALID) {
                continue;
            }
            addFileStore(fs, true);
        }
    }

    public void dump(DataOutputStream out) throws IOException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            for (FileStore fileStore : fileStores.values()) {
                String s = JsonFormat.printer().print(fileStore.toDebugProtobuf()) + "\n";
                out.writeBytes(s);
            }
        }
    }

    // FOR TEST
    public void clear() {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            fileStores.clear();
        }
    }
}
