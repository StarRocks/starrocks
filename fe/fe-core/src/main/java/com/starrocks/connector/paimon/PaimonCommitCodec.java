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

package com.starrocks.connector.paimon;

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.thrift.TPaimonCommitInfo;
import com.starrocks.thrift.TSinkCommitInfo;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.utils.FileStorePathFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Decodes the public native SerializeList protocol, never Java object serialization. */
public final class PaimonCommitCodec {
    // Bound untrusted report input before the SDK allocates lists from embedded lengths.
    private static final int MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;
    private static final int MAX_MESSAGES = 100_000;
    private static final int MAX_PRODUCERS = 100_000;

    private PaimonCommitCodec() {
    }

    private record ProducerFiles(Set<String> ownedFiles, List<CommitMessage> messages) {
    }

    public static final class CollectedMessages {
        private final List<CommitMessage> messages;
        private final List<ProducerFiles> producers;

        private CollectedMessages(List<CommitMessage> messages, List<ProducerFiles> producers) {
            this.messages = List.copyOf(messages);
            this.producers = List.copyOf(producers);
        }

        /** Call only after validating the freshly loaded target, and before any SDK commit or abort. */
        public List<CommitMessage> validateOwnership(FileStoreTable table) {
            if (messages.isEmpty()) {
                return messages;
            }
            try {
                FileStorePathFactory paths = table.store().pathFactory();
                for (ProducerFiles producer : producers) {
                    for (CommitMessage message : producer.messages()) {
                        var files = paths.createDataFilePathFactory(message.partition(), message.bucket());
                        for (DataFileMeta file : ((CommitMessageImpl) message).newFilesIncrement().newFiles()) {
                            // SDK partition escaping, NULL markers and bucket/data-directory layout are authoritative.
                            if (!producer.ownedFiles().contains(files.toPath(file).toString())) {
                                throw new StarRocksConnectorException(
                                        "Paimon committable file has no exact operation ownership receipt");
                            }
                        }
                    }
                }
                return messages;
            } catch (StarRocksConnectorException e) {
                throw e;
            } catch (RuntimeException e) {
                // Malformed typed partitions must not expose SDK paths or reach a mutating SDK call.
                throw new StarRocksConnectorException("Cannot validate Paimon committable file ownership");
            }
        }
    }

    public static List<CommitMessage> decode(int version, byte[] payload) {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        if (version != serializer.getVersion()) {
            throw new StarRocksConnectorException("Unsupported Paimon native commit serializer version: %s", version);
        }
        if (payload == null || payload.length < Integer.BYTES || payload.length > MAX_PAYLOAD_BYTES) {
            throw new StarRocksConnectorException("Invalid Paimon native commit payload size");
        }
        int count = ByteBuffer.wrap(payload).getInt();
        if (count < 0 || count > MAX_MESSAGES || count > payload.length / Integer.BYTES) {
            throw new StarRocksConnectorException("Invalid Paimon native commit message count");
        }
        try {
            DataInputDeserializer input = new DataInputDeserializer(payload);
            List<CommitMessage> messages = serializer.deserializeList(version, input);
            if (messages.size() != count || input.available() != 0) {
                throw new StarRocksConnectorException("Paimon native commit payload has trailing or missing data");
            }
            return messages;
        } catch (IOException | RuntimeException e) {
            if (e instanceof StarRocksConnectorException connectorException) {
                throw connectorException;
            }
            throw new StarRocksConnectorException("Cannot decode Paimon native commit payload", e);
        }
    }

    public static CollectedMessages collect(List<TSinkCommitInfo> reports, PaimonSinkExtra operation) {
        if (reports == null || reports.size() > MAX_PRODUCERS) {
            throw new StarRocksConnectorException("Invalid Paimon producer report count");
        }
        Map<String, TPaimonCommitInfo> producers = new LinkedHashMap<>();
        Map<String, byte[]> digests = new LinkedHashMap<>();
        List<CommitMessage> messages = new ArrayList<>();
        List<ProducerFiles> producerFiles = new ArrayList<>();
        long totalBytes = 0;
        for (TSinkCommitInfo report : reports) {
            if (report == null || !report.isSetPaimon_commit_info()) {
                throw new StarRocksConnectorException("Paimon write received a non-Paimon producer report");
            }
            TPaimonCommitInfo info = report.getPaimon_commit_info();
            if (!info.isSetOperation_id() || !operation.getOperationId().equals(info.getOperation_id())
                    || !info.isSetTable_identity() || !operation.getTableIdentity().equals(info.getTable_identity())
                    || !info.isSetSchema_id() || operation.getSchemaId() != info.getSchema_id()
                    || !info.isSetProducer_id() || info.getProducer_id().isEmpty()
                    || !info.isSetAttempt_id() || info.getAttempt_id() < 0
                    || !info.isSetSerializer_version() || !info.isSetCommit_messages()
                    || !info.isSetPrepare_complete() || !info.isPrepare_complete()
                    || !info.isSetRow_count() || info.getRow_count() < 0) {
                throw new StarRocksConnectorException("Incomplete or mismatched Paimon producer report");
            }
            byte[] payload = info.getCommit_messages();
            if (payload == null || payload.length > MAX_PAYLOAD_BYTES) {
                throw new StarRocksConnectorException("Invalid Paimon producer payload size");
            }
            byte[] digest = digest(payload);
            TPaimonCommitInfo previous = producers.get(info.getProducer_id());
            if (previous != null) {
                if (previous.getAttempt_id() != info.getAttempt_id()
                        || previous.getSerializer_version() != info.getSerializer_version()
                        || previous.getRow_count() != info.getRow_count()
                        || !Objects.equals(previous.getOwned_files(), info.getOwned_files())
                        || !MessageDigest.isEqual(digests.get(info.getProducer_id()), digest)) {
                    throw new StarRocksConnectorException("Conflicting Paimon producer attempts or duplicate payloads");
                }
                continue;
            }
            totalBytes += payload.length;
            if (totalBytes > MAX_PAYLOAD_BYTES) {
                throw new StarRocksConnectorException("Paimon operation commit payload exceeds the report limit");
            }
            List<CommitMessage> decoded = decode(info.getSerializer_version(), payload);
            if (info.getRow_count() > 0 && decoded.isEmpty()) {
                throw new StarRocksConnectorException("Paimon producer reported rows without committable data");
            }
            validateWriteOnlyMessages(decoded, info, operation);
            producers.put(info.getProducer_id(), info);
            digests.put(info.getProducer_id(), digest);
            producerFiles.add(new ProducerFiles(info.isSetOwned_files()
                    ? Set.copyOf(info.getOwned_files()) : Set.of(), List.copyOf(decoded)));
            messages.addAll(decoded);
            if (messages.size() > MAX_MESSAGES) {
                throw new StarRocksConnectorException("Paimon operation has too many commit messages");
            }
        }
        return new CollectedMessages(messages, producerFiles);
    }

    private static void validateWriteOnlyMessages(List<CommitMessage> messages, TPaimonCommitInfo info,
                                                  PaimonSinkExtra operation) {
        // This connector requests write-only operation-local files. Do not let malformed or
        // misrouted reports delete existing files, publish another schema, or compact shared data.
        String root = operation.getLocation() + (operation.getLocation().endsWith("/") ? "" : "/");
        // This is only a cheap structural precheck, not proof of ownership. Exact paths are
        // checked with the validated table's SDK path factory before exposing messages to the caller.
        if (info.isSetOwned_files() && (info.getOwned_files().size() > MAX_MESSAGES
                || info.getOwned_files().stream().anyMatch(path -> path == null || !path.startsWith(root)))) {
            throw new StarRocksConnectorException("Invalid Paimon file ownership receipts");
        }
        for (CommitMessage message : messages) {
            if (!(message instanceof CommitMessageImpl nativeMessage)
                    || !nativeMessage.newFilesIncrement().deletedFiles().isEmpty()
                    || !nativeMessage.newFilesIncrement().changelogFiles().isEmpty()
                    || !nativeMessage.newFilesIncrement().newIndexFiles().isEmpty()
                    || !nativeMessage.newFilesIncrement().deletedIndexFiles().isEmpty()
                    || !nativeMessage.compactIncrement().isEmpty()) {
                throw new StarRocksConnectorException("Paimon producer returned unsupported shared-file changes");
            }
            if (message.partition().getFieldCount() != operation.getPartitionColumns().size()
                    || operation.getBucketCount() > 0 && (message.bucket() < 0
                    || message.bucket() >= operation.getBucketCount())
                    || operation.getBucketCount() == -1 && message.bucket() != 0
                    || message.totalBuckets() != null && message.totalBuckets() != operation.getBucketCount()) {
                throw new StarRocksConnectorException("Paimon producer partition or bucket contract is inconsistent");
            }
            for (DataFileMeta file : nativeMessage.newFilesIncrement().newFiles()) {
                if (file.schemaId() != operation.getSchemaId() || file.rowCount() < 0
                        || file.fileName().isEmpty() || file.fileName().contains("/")
                        || file.fileName().contains("\\") || file.fileName().equals(".") || file.fileName().equals("..")) {
                    throw new StarRocksConnectorException("Paimon producer returned invalid file metadata");
                }
                if (file.externalPath().isPresent() || !file.extraFiles().isEmpty()) {
                    throw new StarRocksConnectorException(
                            "Paimon native writes do not support external-path or auxiliary-file committables");
                }
                if (!info.isSetOwned_files() || info.getOwned_files().isEmpty()) {
                    throw new StarRocksConnectorException("Paimon committable file has no operation ownership receipt");
                }
            }
        }
    }

    private static byte[] digest(byte[] payload) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(payload);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
