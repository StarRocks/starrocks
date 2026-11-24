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

package com.starrocks.persist.image;

import com.google.protobuf.ByteString;
import com.starrocks.common.Config;
import com.starrocks.encryption.EncryptionKey;
import com.starrocks.encryption.KeyMgr;
import com.starrocks.encryption.NormalKey;
import com.starrocks.metric.GaugeMetricImpl;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.ImageEncryptionKeyPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.NormalEncryptionKeyPB;
import com.starrocks.proto.EncryptionAlgorithmPB;
import com.starrocks.proto.EncryptionKeyPB;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KeyMgrImageV3Test {
    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        LoadImageTestSupport.global();
    }

    static NormalKey master(long id) {
        byte[] plain = new byte[16];
        Arrays.fill(plain, (byte) (id + 1));
        NormalKey key = new NormalKey(EncryptionAlgorithmPB.AES_128, plain, null);
        key.setId(id);
        setField(key, "createTime", 123456789L + id);
        return key;
    }

    static void add(KeyMgr manager, EncryptionKey key) {
        EncryptionKeyPB pb = new EncryptionKeyPB();
        key.toPB(pb, manager);
        manager.replayAddKey(pb);
    }

    private KeyMgr restore(KeyMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        KeyMgr restored = new KeyMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(300, sections.get(0).getType());
        assertEquals(source.numKeys(), sections.get(0).getNumEntries());
        assertEquals(0, sections.get(0).getManagerLength());
        return restored;
    }

    private EncryptionKeyPB legacy(EncryptionKey key, KeyMgr manager) {
        EncryptionKeyPB pb = new EncryptionKeyPB();
        key.toPB(pb, manager);
        return pb;
    }

    @Test
    public void testMasterAndEncryptedChildRecoverAndDecryptAsV2() throws Exception {
        KeyMgr source = new KeyMgr();
        NormalKey root = master(1);
        NormalKey child = (NormalKey) root.generateKey();
        child.setId(2);
        setField(child, "createTime", 234567890L);
        byte[] expectedPlain = child.getPlainKey().clone();
        add(source, root);
        add(source, child);
        KeyMgr expected = new KeyMgr();
        FinalManagersImageTestSupport.v2(source::save, expected::load);
        KeyMgr restored = restore(source);
        NormalKey restoredRoot = (NormalKey) restored.getKeyById(1);
        NormalKey restoredChild = (NormalKey) restored.getKeyById(2);
        assertSame(restoredRoot, restoredChild.getParent());
        assertTrue(restoredRoot.isMasterKey());
        assertTrue(restoredChild.isKEK());
        assertSame(restoredChild, restored.getCurrentKEK());
        assertNull(restoredChild.getPlainKey());
        assertArrayEquals(child.getEncryptedKey(), restoredChild.getEncryptedKey());
        assertEquals(234567890L, restoredChild.getCreateTime());
        GsonEquivalence.assertEquivalent(legacy(expected.getKeyById(1), expected), legacy(restoredRoot, restored));
        GsonEquivalence.assertEquivalent(legacy(expected.getKeyById(2), expected), legacy(restoredChild, restored));
        restoredRoot.decryptKey(restoredChild);
        assertArrayEquals(expectedPlain, restoredChild.getPlainKey());
        NormalKey leaf = (NormalKey) restoredChild.generateKey();
        byte[] leafPlain = leaf.getPlainKey().clone();
        leaf.setPlainKey(null);
        restoredChild.decryptKey(leaf);
        assertArrayEquals(leafPlain, leaf.getPlainKey());

    }

    @Test
    public void testWriterUsesActualNormalInstanceAndAvoidsPersistingCachedPlainChild() {
        KeyMgr manager = new KeyMgr();
        NormalKey root = master(1);
        ImageEncryptionKeyPB rootPb = root.toProto(manager);
        assertEquals(ImageEncryptionKeyPB.Kind.NORMAL, rootPb.getKind());
        assertEquals(ImageEncryptionKeyPB.InstanceCase.NORMAL, rootPb.getInstanceCase());
        assertTrue(rootPb.getNormal().hasPlainKey());
        assertFalse(rootPb.getNormal().hasEncryptedKey());
        NormalKey child = (NormalKey) root.generateKey();
        child.setId(2);
        ImageEncryptionKeyPB childPb = child.toProto(manager);
        assertTrue(childPb.getNormal().hasEncryptedKey());
        assertFalse(childPb.getNormal().hasPlainKey());
        assertEquals(1, childPb.getParentId());
        assertEquals(NormalEncryptionKeyPB.Algorithm.AES_128, childPb.getNormal().getAlgorithm());
    }

    @Test
    public void testKeysAreWrittenInExistingTreeMapOrder() throws Exception {
        KeyMgr source = new KeyMgr();
        add(source, master(103));
        add(source, master(1));
        add(source, master(51));
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        java.util.ArrayList<Long> ids = new java.util.ArrayList<>();
        LoadImageTestSupport.load(path, List.of(com.starrocks.common.Pair.create(SectionId.KEY_MGR,
                reader -> reader.readEntries(ImageEncryptionKeyPB.parser(), pb -> ids.add(pb.getId())))));
        assertEquals(List.of(1L, 51L, 103L), ids);
        KeyMgr restored = restore(source);
        assertEquals(103L, restored.getCurrentKEK().getId());

    }

    @Test
    public void testMissingParentFailsWithoutInstallingChild() throws Exception {
        NormalKey root = master(1);
        NormalKey child = (NormalKey) root.generateKey();
        child.setId(2);
        Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.KEY_MGR,
                List.of(child.toProto(new KeyMgr())));
        KeyMgr restored = new KeyMgr();
        Exception error = assertThrows(Exception.class,
                () -> LoadImageTestSupport.load(path, restored.getImageV3Loaders()));
        assertTrue(error.toString().contains("parent") || error.getCause() != null);
        assertEquals(0, restored.numKeys());
        add(restored, root);
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertSame(restored.getKeyById(1), restored.getKeyById(2).getParent());

    }

    @Test
    public void testMissingAlgorithmAndKeyBytesAreNotSubtypeErrors() {
        ImageEncryptionKeyPB pb = master(1).toProto(new KeyMgr());
        assertThrows(IllegalArgumentException.class, () -> EncryptionKey.fromProto(pb.toBuilder()
                .setNormal(pb.getNormal().toBuilder().clearAlgorithm()).build(), new KeyMgr()));
        assertThrows(IllegalArgumentException.class, () -> EncryptionKey.fromProto(pb.toBuilder()
                .setNormal(pb.getNormal().toBuilder().clearPlainKey().clearEncryptedKey()).build(), new KeyMgr()));
        assertThrows(IllegalArgumentException.class, () -> EncryptionKey.fromProto(pb.toBuilder()
                .setNormal(pb.getNormal().toBuilder().setAlgorithmValue(999)).build(), new KeyMgr()));
    }

    @Test
    public void testEmptyBytesAndOptionalZeroFieldsFollowLegacyConversion() {
        NormalKey source = new NormalKey(EncryptionAlgorithmPB.NO_ENCRYPTION, new byte[0], null);
        setField(source, "createTime", 0L);
        KeyMgr manager = new KeyMgr();
        ImageEncryptionKeyPB pb = source.toProto(manager);
        assertFalse(pb.hasId());
        assertFalse(pb.hasParentId());
        assertFalse(pb.hasCreateTime());
        assertTrue(pb.getNormal().hasPlainKey());
        assertEquals(ByteString.EMPTY, pb.getNormal().getPlainKey());
        NormalKey restored = (NormalKey) EncryptionKey.fromProto(pb, manager);
        assertEquals(0L, restored.getId());
        assertNull(restored.getParent());
        assertArrayEquals(new byte[0], restored.getPlainKey());
        assertTrue(restored.getCreateTime() > 0);
        ImageEncryptionKeyPB explicitZero = pb.toBuilder().setId(0).setCreateTime(0).build();
        assertEquals(0L, EncryptionKey.fromProto(explicitZero, manager).getCreateTime());
    }

    @Test
    public void testUnknownKindCheckedBeforeParentOrPayload() {
        ImageEncryptionKeyPB unknown = ImageEncryptionKeyPB.newBuilder().setKindValue(991).setParentId(999).build();
        assertThrows(SubtypeNotFoundException.class, () -> EncryptionKey.fromProto(unknown, new KeyMgr()));
        assertThrows(IllegalArgumentException.class,
                () -> EncryptionKey.fromProto(ImageEncryptionKeyPB.getDefaultInstance(), new KeyMgr()));
    }

    @Test
    public void testReaderSkipsUnknownEntryAndContinuesWithNextKnownKey() throws Exception {
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.KEY_MGR, List.of(
                    master(1).toProto(new KeyMgr()), ImageEncryptionKeyPB.newBuilder().setKindValue(999).build(),
                    master(3).toProto(new KeyMgr())));
            Config.metadata_ignore_unknown_subtype = false;
            KeyMgr strict = new KeyMgr();
            assertThrows(Exception.class, () -> LoadImageTestSupport.load(path, strict.getImageV3Loaders()));
            assertEquals(1, strict.numKeys());
            Config.metadata_ignore_unknown_subtype = true;
            KeyMgr tolerant = new KeyMgr();
            LoadImageTestSupport.load(path, tolerant.getImageV3Loaders());
            assertEquals(2, tolerant.numKeys());
            assertNotNull(tolerant.getKeyById(3));

        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }

    @Test
    public void testIgnoringUnknownParentDoesNotSilentlyDropDependentKnownChild() throws Exception {
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            Config.metadata_ignore_unknown_subtype = true;
            NormalKey child = (NormalKey) master(1).generateKey();
            child.setId(2);
            Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.KEY_MGR,
                    List.of(ImageEncryptionKeyPB.newBuilder().setKindValue(999).setId(1).build(),
                            child.toProto(new KeyMgr())));
            KeyMgr restored = new KeyMgr();
            assertThrows(Exception.class, () -> LoadImageTestSupport.load(path, restored.getImageV3Loaders()));
            assertEquals(0, restored.numKeys());
        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }

    @Test
    public void testDuplicateIdReplacesKeyAndEmptyImageKeepsExistingKeys() throws Exception {
        NormalKey first = master(1);
        NormalKey second = master(1);
        setField(second, "createTime", 999L);
        Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.KEY_MGR,
                List.of(first.toProto(new KeyMgr()), second.toProto(new KeyMgr())));
        KeyMgr restored = new KeyMgr();
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(1, restored.numKeys());
        assertEquals(999L, restored.getKeyById(1).getCreateTime());
        Path empty = LoadImageTestSupport.save(tempDir, new KeyMgr()::save);
        LoadImageTestSupport.load(empty, restored.getImageV3Loaders());
        assertEquals(1, restored.numKeys());

    }

    @Test
    public void testLargeKeyList() throws Exception {
        KeyMgr source = new KeyMgr();
        for (int id = 1; id <= 1024; id++) {
            add(source, master(id));
        }
        KeyMgr restored = restore(source);
        assertEquals(1024, restored.numKeys());
        assertEquals(1024L, restored.getCurrentKEK().getId());

    }

    @Test
    public void testLoadedKeyGaugeCountsAcceptedEntriesAndKeepsV2FailureBehavior() throws Exception {
        boolean previousInit = MetricRepo.hasInit;
        boolean previousIgnore = Config.metadata_ignore_unknown_subtype;
        GaugeMetricImpl<Long> previousGauge = MetricRepo.GAUGE_ENCRYPTION_KEY_NUM;
        GaugeMetricImpl<Long> gauge = new GaugeMetricImpl<>("test_key_count", Metric.MetricUnit.NOUNIT, "test", 99L);
        MetricRepo.GAUGE_ENCRYPTION_KEY_NUM = gauge;
        MetricRepo.hasInit = true;
        try {
            Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.KEY_MGR, List.of(
                    master(1).toProto(new KeyMgr()), ImageEncryptionKeyPB.newBuilder().setKindValue(900).build(),
                    master(3).toProto(new KeyMgr())));
            Config.metadata_ignore_unknown_subtype = true;
            KeyMgr restored = new KeyMgr();
            LoadImageTestSupport.load(path, restored.getImageV3Loaders());
            assertEquals(2L, gauge.getValue());
            assertEquals(2, restored.numKeys());
            gauge.setValue(99L);
            Config.metadata_ignore_unknown_subtype = false;
            KeyMgr strict = new KeyMgr();
            assertThrows(Exception.class, () -> LoadImageTestSupport.load(path, strict.getImageV3Loaders()));
            assertEquals(1, strict.numKeys());
            assertEquals(99L, gauge.getValue());
            Path empty = LoadImageTestSupport.save(tempDir, new KeyMgr()::save);
            KeyMgr fresh = new KeyMgr();
            LoadImageTestSupport.load(empty, fresh.getImageV3Loaders());
            assertEquals(0L, gauge.getValue());

        } finally {
            MetricRepo.hasInit = previousInit;
            MetricRepo.GAUGE_ENCRYPTION_KEY_NUM = previousGauge;
            Config.metadata_ignore_unknown_subtype = previousIgnore;
        }
    }
}
