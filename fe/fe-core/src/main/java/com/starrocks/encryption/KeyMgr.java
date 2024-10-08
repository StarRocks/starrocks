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
package com.starrocks.encryption;

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.google.common.base.Preconditions;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.proto.EncryptionKeyPB;
import com.starrocks.proto.EncryptionMetaPB;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetKeysRequest;
import com.starrocks.thrift.TGetKeysResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KeyMgr {
    private static final Logger LOG = LogManager.getLogger(KeyMgr.class);
    private static Codec<EncryptionKeyPB> encryptionKeyPBCodec = ProtobufProxy.create(EncryptionKeyPB.class);
    private static Codec<EncryptionMetaPB> encryptionMetaPBCodec = ProtobufProxy.create(EncryptionMetaPB.class);

    public static final long DEFAULT_MASTER_KYE_ID = 1;

    private final ReentrantReadWriteLock keysLock = new ReentrantReadWriteLock();
    private final TreeMap<Long, EncryptionKey> idToKey = new TreeMap<>();

    public static boolean isEncrypted() {
        return !Config.default_master_key.isEmpty();
    }

    public int numKeys() {
        keysLock.readLock().lock();
        try {
            return idToKey.size();
        } finally {
            keysLock.readLock().unlock();
        }
    }

    public EncryptionKey getKeyById(long id) {
        keysLock.readLock().lock();
        try {
            return idToKey.get(id);
        } finally {
            keysLock.readLock().unlock();
        }
    }

    private void addKey(EncryptionKey key) {
        idToKey.put(key.id, key);
        EncryptionKeyPB pb = new EncryptionKeyPB();
        key.toPB(pb, this);
        GlobalStateMgr.getCurrentState().getEditLog().logAddKey(pb);
    }

    public void replayAddKey(EncryptionKeyPB keyPB) {
        EncryptionKey key = create(keyPB);
        keysLock.writeLock().lock();
        try {
            idToKey.put(key.id, key);
        } finally {
            keysLock.writeLock().unlock();
        }
    }

    protected EncryptionKey generateNewKEK() {
        EncryptionKey masterKey = idToKey.get(DEFAULT_MASTER_KYE_ID);
        EncryptionKey kek = masterKey.generateKey();
        kek.id = idToKey.lastKey() + 1;
        addKey(kek);
        if (MetricRepo.hasInit) {
            MetricRepo.GAUGE_ENCRYPTION_KEY_NUM.setValue((long) idToKey.size());
        }
        LOG.info(String.format("generate new KEK %s, total: %d", kek.toString(), idToKey.size()));
        return kek;
    }

    public void initDefaultMasterKey() {
        String defaultMasterKeySpec = Config.default_master_key;
        GlobalVariable.enableTde = isEncrypted();
        keysLock.writeLock().lock();
        try {
            if (defaultMasterKeySpec.isEmpty()) {
                if (idToKey.size() != 0) {
                    LOG.error("default_master_key removed in config");
                    System.exit(1);
                }
            } else {
                EncryptionKey masterKeyFromConfig = EncryptionKey.createFromSpec(defaultMasterKeySpec);
                if (idToKey.size() == 0) {
                    // setup default master key
                    masterKeyFromConfig.id = DEFAULT_MASTER_KYE_ID;
                    addKey(masterKeyFromConfig);
                    LOG.info("create default master key:" + masterKeyFromConfig);
                } else {
                    // check masterkey not changed
                    EncryptionKey masterKey = idToKey.get(DEFAULT_MASTER_KYE_ID);
                    Preconditions.checkState(masterKey.equals(masterKeyFromConfig),
                            "default_master_key changed meta:%s config:%s", masterKey.toSpec(),
                            masterKeyFromConfig.toSpec());
                }
                if (idToKey.size() == 1) {
                    // setup first KEK
                    generateNewKEK();
                }
            }
        } catch (Exception e) {
            LOG.fatal("init default master key failed, will exit.", e);
            System.exit(-1);
        } finally {
            keysLock.writeLock().unlock();
        }
    }

    public void checkKeyRotation() {
        if (!isEncrypted()) {
            return;
        }
        long keyValidSec = Config.key_rotation_days * 24 * 3600;
        keysLock.writeLock().lock();
        try {
            if (idToKey.isEmpty()) {
                return;
            }
            EncryptionKey lastKEK = idToKey.lastEntry().getValue();
            Preconditions.checkState(lastKEK.isKEK(), "should be KEK:" + lastKEK);
            long now = System.currentTimeMillis() / 1000;
            if (lastKEK.createTime + keyValidSec <= now) {
                generateNewKEK();
            }
            if (MetricRepo.hasInit) {
                MetricRepo.GAUGE_ENCRYPTION_KEY_NUM.setValue((long) idToKey.size());
            }
        } finally {
            keysLock.writeLock().unlock();
        }
    }

    public EncryptionKey getCurrentKEK() {
        keysLock.readLock().lock();
        try {
            if (idToKey.isEmpty()) {
                // not encrypted, do nothing
                return null;
            }
            return idToKey.lastEntry().getValue();
        } finally {
            keysLock.readLock().unlock();
        }
    }

    public EncryptionKey create(EncryptionKeyPB pb) {
        EncryptionKey key;
        switch (pb.type) {
            case NORMAL_KEY:
                key = new NormalKey();
                break;
            default:
                throw new IllegalStateException("Unexpected EncryptionKeyTypePB value: " + pb.type);
        }
        key.fromPB(pb, this);
        return key;
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        keysLock.writeLock().lock();
        try {
            reader.readCollection(EncryptionKeyPB.class, pb -> {
                EncryptionKey key = create(pb);
                idToKey.put(key.id, key);
            });
            LOG.info("loaded {} keys", idToKey.size());

            if (MetricRepo.hasInit) {
                MetricRepo.GAUGE_ENCRYPTION_KEY_NUM.setValue((long) idToKey.size());
            }
        } finally {
            keysLock.writeLock().unlock();
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        keysLock.readLock().lock();
        try {
            final int cnt = 1 + idToKey.size();
            SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.KEY_MGR, cnt);
            // write keys
            writer.writeInt(idToKey.size());
            for (EncryptionKey key : idToKey.values()) {
                EncryptionKeyPB pb = new EncryptionKeyPB();
                key.toPB(pb, this);
                writer.writeJson(pb);
            }
            writer.close();
        } finally {
            keysLock.readLock().unlock();
        }
    }

    public TGetKeysResponse getKeys(TGetKeysRequest req) throws IOException {
        // for now, just return current KEK and master key encryption meta
        TGetKeysResponse ret = new TGetKeysResponse();
        if (!isEncrypted()) {
            return ret;
        }
        EncryptionKey kek = getCurrentKEK();
        if (kek == null) {
            return ret;
        }
        EncryptionKey root = kek.getParent();
        EncryptionMetaPB pb = new EncryptionMetaPB();
        pb.keyHierarchy = new ArrayList<>();
        EncryptionKeyPB rootPb = new EncryptionKeyPB();
        root.toPB(rootPb, this);
        pb.keyHierarchy.add(rootPb);
        EncryptionKeyPB kekPb = new EncryptionKeyPB();
        kek.toPB(kekPb, this);
        pb.keyHierarchy.add(kekPb);
        try {
            byte[] bytes = encryptionMetaPBCodec.encode(pb);
            ret.addToKey_metas(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
            throw new IOException("encode EncryptionMetaPB failed", e);
        }
        return ret;
    }

    public byte[] getCurrentKEKAsEncryptionMeta() throws AnalysisException {
        if (!isEncrypted()) {
            return null;
        }
        EncryptionKey kek = getCurrentKEK();
        if (kek == null) {
            throw new AnalysisException("encryption key not setup");
        }
        EncryptionMetaPB pb = new EncryptionMetaPB();
        pb.keyHierarchy = new ArrayList();
        EncryptionKey root = kek.getParent();
        EncryptionKeyPB rootPb = new EncryptionKeyPB();
        root.toPB(rootPb, this);
        pb.keyHierarchy.add(rootPb);
        EncryptionKeyPB kekPb = new EncryptionKeyPB();
        kek.toPB(kekPb, this);
        pb.keyHierarchy.add(kekPb);
        try {
            return encryptionMetaPBCodec.encode(pb);
        } catch (IOException e) {
            throw new AnalysisException("encode EncryptionMetaPB failed", e);
        }
    }
}
