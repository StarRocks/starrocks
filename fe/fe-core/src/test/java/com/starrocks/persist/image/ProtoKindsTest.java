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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.authentication.GroupProvider;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authorization.PrivilegeObjectProtoConverter;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.SnapshotInfo;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RecyclePartitionInfo;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRangeProtoConverter;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadProgress;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.AbstractJobPB;
import com.starrocks.persist.proto.AlterJobPB;
import com.starrocks.persist.proto.AnalyzeJobPB;
import com.starrocks.persist.proto.AnalyzeStatusPB;
import com.starrocks.persist.proto.ComputeResourcePB;
import com.starrocks.persist.proto.DistributionInfoPB;
import com.starrocks.persist.proto.FunctionPB;
import com.starrocks.persist.proto.GroupProviderPB;
import com.starrocks.persist.proto.LoadJobPB;
import com.starrocks.persist.proto.PartitionInfoPB;
import com.starrocks.persist.proto.PrivilegeObjectPB;
import com.starrocks.persist.proto.RecyclePartitionInfoPB;
import com.starrocks.persist.proto.ResourcePB;
import com.starrocks.persist.proto.RoutineLoadJobPB;
import com.starrocks.persist.proto.RoutineLoadProgressPB;
import com.starrocks.persist.proto.SecurityIntegrationPB;
import com.starrocks.persist.proto.SnapshotInfoPB;
import com.starrocks.persist.proto.TablePB;
import com.starrocks.persist.proto.TabletPB;
import com.starrocks.persist.proto.TvrVersionRangePB;
import com.starrocks.persist.proto.TxnCommitAttachmentPB;
import com.starrocks.persist.proto.TypePB;
import com.starrocks.persist.proto.VariantPB;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.transaction.TxnCommitAttachment;
import com.starrocks.type.TypeProtoConverter;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProtoKindsTest {
    @FunctionalInterface
    interface Converter<P> {
        Object convert(P pb) throws Exception;
    }

    record Fixture(Message prototype, Converter<Message> converter) {
        Message withKind(int number) {
            Descriptors.FieldDescriptor kind = prototype.getDescriptorForType().findFieldByName("kind");
            return prototype.toBuilder().setField(kind, kind.getEnumType().findValueByNumberCreatingIfUnknown(number)).build();
        }

        @Override
        public String toString() {
            return prototype.getDescriptorForType().getName();
        }
    }

    @SuppressWarnings("unchecked")
    private static <P extends Message> Fixture fixture(P prototype, Converter<P> converter) {
        return new Fixture(prototype, message -> converter.convert((P) message));
    }

    static Stream<Fixture> fixtures() {
        return Stream.of(
                fixture(AbstractJobPB.getDefaultInstance(), AbstractJob::fromProto),
                fixture(AlterJobPB.getDefaultInstance(), AlterJobV2::fromProto),
                fixture(AnalyzeJobPB.getDefaultInstance(), AnalyzeJob::fromProto),
                fixture(AnalyzeStatusPB.getDefaultInstance(), AnalyzeStatus::fromProto),
                fixture(ComputeResourcePB.getDefaultInstance(), ComputeResource::fromProto),
                fixture(DistributionInfoPB.getDefaultInstance(), DistributionInfo::fromProto),
                fixture(FunctionPB.getDefaultInstance(), Function::fromProto),
                fixture(GroupProviderPB.getDefaultInstance(), GroupProvider::fromProto),
                fixture(LoadJobPB.getDefaultInstance(), LoadJob::fromProto),
                fixture(PartitionInfoPB.getDefaultInstance(), PartitionInfo::fromProto),
                fixture(PrivilegeObjectPB.getDefaultInstance(), PrivilegeObjectProtoConverter::fromProto),
                fixture(RecyclePartitionInfoPB.getDefaultInstance(), RecyclePartitionInfo::fromProto),
                fixture(ResourcePB.getDefaultInstance(), Resource::fromProto),
                fixture(RoutineLoadJobPB.getDefaultInstance(), RoutineLoadJob::fromProto),
                fixture(RoutineLoadProgressPB.getDefaultInstance(), RoutineLoadProgress::fromProto),
                fixture(SecurityIntegrationPB.getDefaultInstance(), SecurityIntegration::fromProto),
                fixture(SnapshotInfoPB.getDefaultInstance(), SnapshotInfo::fromProto),
                fixture(TablePB.getDefaultInstance(), Table::fromProto),
                fixture(TabletPB.getDefaultInstance(), Tablet::fromProto),
                fixture(TvrVersionRangePB.getDefaultInstance(), TvrVersionRange::fromProto),
                fixture(TxnCommitAttachmentPB.getDefaultInstance(), TxnCommitAttachment::fromProto),
                fixture(TypePB.getDefaultInstance(), TypeProtoConverter::fromProto),
                fixture(VariantPB.getDefaultInstance(), TabletRangeProtoConverter::fromProto));
    }

    @ParameterizedTest(name = "{0}: unknown kinds before any payload conversion")
    @MethodSource("fixtures")
    public void testUnknownKinds(Fixture fixture) throws Exception {
        boolean original = Config.metadata_ignore_unknown_subtype;
        try {
            for (boolean ignore : new boolean[] {false, true}) {
                Config.metadata_ignore_unknown_subtype = ignore;
                for (int number : new int[] {-1, 999, Integer.MAX_VALUE}) {
                    Message message = fixture.withKind(number);
                    // Use real protobuf parsing, including unknown negative enum values.
                    Message parsed = fixture.prototype().getParserForType().parseFrom(message.toByteArray());
                    SubtypeNotFoundException failure = assertThrows(SubtypeNotFoundException.class,
                            () -> fixture.converter().convert(parsed));
                    assertEquals(Integer.toString(number), failure.getSubtype());
                    assertTrue(failure.getMessage().contains(fixture.toString()), failure.getMessage());
                }
            }
        } finally {
            Config.metadata_ignore_unknown_subtype = original;
        }
    }

    @ParameterizedTest(name = "{0}: unspecified kinds are malformed")
    @MethodSource("fixtures")
    public void testMissingKindsAreNotUnknownSubtypes(Fixture fixture) {
        boolean original = Config.metadata_ignore_unknown_subtype;
        Config.metadata_ignore_unknown_subtype = true;
        try {
            assertThrows(IllegalArgumentException.class, () -> fixture.converter().convert(fixture.prototype()));
            assertThrows(IllegalArgumentException.class, () -> fixture.converter().convert(fixture.withKind(0)));
        } finally {
            Config.metadata_ignore_unknown_subtype = original;
        }
    }
}
