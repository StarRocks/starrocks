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
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import com.starrocks.alter.reshard.ReshardingTablet;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.encryption.EncryptionKey;
import com.starrocks.encryption.KeyMgr;
import com.starrocks.lake.snapshot.ClusterSnapshotJob;
import com.starrocks.load.pipe.Pipe;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.ClusterSnapshotJobPB;
import com.starrocks.persist.proto.ImageEncryptionKeyPB;
import com.starrocks.persist.proto.PipePB;
import com.starrocks.persist.proto.ReplicationJobPB;
import com.starrocks.persist.proto.ReshardingTabletPB;
import com.starrocks.persist.proto.TabletReshardJobPB;
import com.starrocks.replication.ReplicationJob;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FinalManagersCompatibilityImageV3Test {
    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        LoadImageTestSupport.global();
    }

    private record PolymorphicCase(Message good, Function<Message, Object> convert) {
    }

    private List<PolymorphicCase> cases() {
        return List.of(
                new PolymorphicCase(ReplicationMgrImageV3Test.job(1, false, 3).toProto(),
                        pb -> ReplicationJob.fromProto((ReplicationJobPB) pb)),
                new PolymorphicCase(ReplicationMgrImageV3Test.job(2, true, 3).toProto(),
                        pb -> ReplicationJob.fromProto((ReplicationJobPB) pb)),
                new PolymorphicCase(KeyMgrImageV3Test.master(1).toProto(new KeyMgr()),
                        pb -> EncryptionKey.fromProto((ImageEncryptionKeyPB) pb, new KeyMgr())),
                new PolymorphicCase(ReshardImageTestSupport.job(1, true, TabletReshardJob.JobState.RUNNING).toProto(),
                        pb -> TabletReshardJob.fromProto((TabletReshardJobPB) pb)),
                new PolymorphicCase(ReshardImageTestSupport.job(2, false, TabletReshardJob.JobState.RUNNING).toProto(),
                        pb -> TabletReshardJob.fromProto((TabletReshardJobPB) pb)),
                new PolymorphicCase(ReshardImageTestSupport.index(
                        ReshardImageTestSupport.job(1, true, TabletReshardJob.JobState.RUNNING))
                        .getReshardingTablets().get(0).toImageProto(),
                        pb -> ReshardingTablet.fromImageProto((ReshardingTabletPB) pb)),
                new PolymorphicCase(ReshardImageTestSupport.index(
                        ReshardImageTestSupport.job(2, false, TabletReshardJob.JobState.RUNNING))
                        .getReshardingTablets().get(0).toImageProto(),
                        pb -> ReshardingTablet.fromImageProto((ReshardingTabletPB) pb)),
                new PolymorphicCase(ReshardImageTestSupport.index(
                        ReshardImageTestSupport.job(1, true, TabletReshardJob.JobState.RUNNING))
                        .getReshardingTablets().get(1).toImageProto(),
                        pb -> ReshardingTablet.fromImageProto((ReshardingTabletPB) pb)));
    }

    private Message futureSubtype(Message prototype) throws Exception {
        // Simulate a future writer: kind 900 and its length-delimited instance on a field unknown to this reader.
        UnknownFieldSet fields = UnknownFieldSet.newBuilder()
                .addField(1, UnknownFieldSet.Field.newBuilder().addVarint(900).build())
                .addField(100, UnknownFieldSet.Field.newBuilder()
                        .addLengthDelimited(ByteString.copyFromUtf8("future instance bytes")).build())
                .build();
        byte[] bytes = prototype.newBuilderForType().setUnknownFields(fields).build().toByteArray();
        return prototype.getParserForType().parseFrom(bytes);
    }

    private Message withUnknownFields(Message source) throws Exception {
        UnknownFieldSet fields = source.getUnknownFields().toBuilder()
                .addField(1000, UnknownFieldSet.Field.newBuilder().addVarint(Long.MAX_VALUE).build())
                .addField(1001, UnknownFieldSet.Field.newBuilder()
                        .addLengthDelimited(ByteString.copyFromUtf8("field added by a newer release")).build())
                .build();
        return source.getParserForType().parseFrom(source.toBuilder().setUnknownFields(fields).build().toByteArray());
    }

    @Test
    public void testUnknownKindAndUnknownOneofPayloadSurviveWireParsingUntilConversion() throws Exception {
        for (PolymorphicCase testCase : cases()) {
            Message parsed = futureSubtype(testCase.good());
            Descriptors.FieldDescriptor kind = parsed.getDescriptorForType().findFieldByName("kind");
            assertEquals(900, ((Descriptors.EnumValueDescriptor) parsed.getField(kind)).getNumber());
            assertTrue(parsed.getUnknownFields().hasField(100));
            assertNull(parsed.getOneofFieldDescriptor(parsed.getDescriptorForType().getOneofs().stream()
                    .filter(oneof -> oneof.getName().equals("instance")).findFirst().orElseThrow()));
            assertThrows(SubtypeNotFoundException.class, () -> testCase.convert().apply(parsed));
        }
    }

    @Test
    public void testKnownSubtypeWithUnknownFieldsKeepsKnownMetadata() throws Exception {
        for (PolymorphicCase testCase : cases()) {
            Message parsed = withUnknownFields(testCase.good());
            assertTrue(parsed.getUnknownFields().hasField(1000));
            assertTrue(parsed.getUnknownFields().hasField(1001));
            Object expected = testCase.convert().apply(testCase.good());
            Object restored = testCase.convert().apply(parsed);
            if (expected instanceof EncryptionKey expectedKey && restored instanceof EncryptionKey restoredKey) {
                assertEquals(expectedKey.toProto(new KeyMgr()), restoredKey.toProto(new KeyMgr()));
            } else {
                GsonEquivalence.assertEquivalent(expected, restored);
            }
        }
    }

    @Test
    public void testWireUnknownSubtypeCanBeSkippedBetweenKnownMessagesForEveryHierarchy() throws Exception {
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            for (PolymorphicCase testCase : cases()) {
                Message future = futureSubtype(testCase.good());
                Path file = FinalManagersImageTestSupport.entries(tempDir, SectionId.REPLICATION_MGR,
                        List.of(testCase.good(), future, testCase.good()));
                List<Object> converted = new ArrayList<>();
                SectionLoader loader = reader -> reader.readEntries(testCase.good().getParserForType(), pb ->
                        converted.add(testCase.convert().apply(pb)));
                Config.metadata_ignore_unknown_subtype = false;
                assertThrows(Exception.class, () -> LoadImageTestSupport.load(file,
                        List.of(Pair.create(SectionId.REPLICATION_MGR, loader))));
                assertEquals(1, converted.size());
                Config.metadata_ignore_unknown_subtype = true;
                converted.clear();
                var sections = LoadImageTestSupport.load(file,
                        List.of(Pair.create(SectionId.REPLICATION_MGR, loader)));
                assertEquals(3, sections.get(0).getNumEntries());
                assertEquals(2, converted.size());
            }

        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }

    @Test
    public void testUnknownFieldsInsideReplicationCoreAndLakeInstance() throws Exception {
        ReplicationJobPB original = ReplicationMgrImageV3Test.job(1, true, 3).toProto();
        var lake = original.getLakeReplication();
        var core = lake.getCore();
        var extendedCore = withUnknownFields(core);
        var extendedLake = lake.toBuilder().setCore((com.starrocks.persist.proto.ReplicationJobCorePB) extendedCore).build();
        ReplicationJobPB extended = original.toBuilder()
                .setLakeReplication((com.starrocks.persist.proto.LakeReplicationJobPB) withUnknownFields(extendedLake)).build();
        GsonEquivalence.assertEquivalent(ReplicationJob.fromProto(original), ReplicationJob.fromProto(extended));
    }

    @Test
    public void testUnknownFieldsInsideEncryptionPayloadDoNotExposeCachedPlainKey() throws Exception {
        KeyMgr manager = new KeyMgr();
        var master = KeyMgrImageV3Test.master(1);
        KeyMgrImageV3Test.add(manager, master);
        var child = master.generateKey();
        child.setId(2);
        ImageEncryptionKeyPB original = child.toProto(manager);
        var extendedNormal = (com.starrocks.persist.proto.NormalEncryptionKeyPB) withUnknownFields(original.getNormal());
        ImageEncryptionKeyPB extended = original.toBuilder().setNormal(extendedNormal).build();
        EncryptionKey restored = EncryptionKey.fromProto(extended, manager);
        assertEquals(original, restored.toProto(manager));
        assertFalse(restored.toProto(manager).getNormal().hasPlainKey());
    }

    @Test
    public void testUnknownFieldsOnConcretePipeAndSnapshotMessages() throws Exception {
        PipePB pipe = PipeMgrImageV3Test.pipe(1, Pipe.State.SUSPEND).toProto();
        PipePB extendedPipe = (PipePB) withUnknownFields(pipe);
        GsonEquivalence.assertEquivalent(Pipe.fromProto(pipe), Pipe.fromProto(extendedPipe));
        ClusterSnapshotJobPB snapshot = ClusterSnapshotMgrImageV3Test.job(1,
                ClusterSnapshotJob.ClusterSnapshotJobState.FINISHED).toProto();
        ClusterSnapshotJobPB extendedSnapshot = (ClusterSnapshotJobPB) withUnknownFields(snapshot);
        GsonEquivalence.assertEquivalent(ClusterSnapshotJob.fromProto(snapshot), ClusterSnapshotJob.fromProto(extendedSnapshot));
    }

    @Test
    public void testMissingKindRemainsMalformedEvenWithUnknownFutureInstance() throws Exception {
        for (PolymorphicCase testCase : cases()) {
            UnknownFieldSet fields = UnknownFieldSet.newBuilder().addField(100,
                    UnknownFieldSet.Field.newBuilder().addLengthDelimited(ByteString.copyFromUtf8("new instance")).build())
                    .build();
            Message parsed = testCase.good().getParserForType().parseFrom(testCase.good().newBuilderForType()
                    .setUnknownFields(fields).build().toByteArray());
            assertThrows(IllegalArgumentException.class, () -> testCase.convert().apply(parsed));
        }
    }
}
