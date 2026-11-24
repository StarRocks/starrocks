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

package com.starrocks.authorization;

import com.google.common.base.Preconditions;
import com.starrocks.persist.image.ProtoKinds;
import com.starrocks.persist.proto.PrivilegeObjectPB;

/** Explicit image conversion; it does not resolve catalog objects or check current grants. */
public final class PrivilegeObjectProtoConverter {
    private PrivilegeObjectProtoConverter() {
    }

    public static PrivilegeObjectPB toProto(PEntryObject object) {
        PrivilegeObjectPB.Builder builder = PrivilegeObjectPB.newBuilder();
        if (object.getClass() == DbPEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.DATABASE)
                    .setDatabase(((DbPEntryObject) object).toProto()).build();
        }
        if (object.getClass() == TablePEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.TABLE)
                    .setTable(((TablePEntryObject) object).toProto()).build();
        }
        if (object.getClass() == UserPEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.USER)
                    .setUser(((UserPEntryObject) object).toProto()).build();
        }
        if (object.getClass() == ResourcePEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.RESOURCE)
                    .setResource(((ResourcePEntryObject) object).toProto()).build();
        }
        if (object.getClass() == ViewPEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.VIEW)
                    .setTable(((ViewPEntryObject) object).toProto()).build();
        }
        if (object.getClass() == MaterializedViewPEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.MATERIALIZED_VIEW)
                    .setTable(((MaterializedViewPEntryObject) object).toProto()).build();
        }
        if (object.getClass() == GlobalFunctionPEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.GLOBAL_FUNCTION)
                    .setGlobalFunction(((GlobalFunctionPEntryObject) object).toProto()).build();
        }
        if (object.getClass() == FunctionPEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.FUNCTION)
                    .setFunction(((FunctionPEntryObject) object).toProto()).build();
        }
        if (object.getClass() == CatalogPEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.CATALOG)
                    .setCatalog(((CatalogPEntryObject) object).toProto()).build();
        }
        if (object.getClass() == ResourceGroupPEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.RESOURCE_GROUP)
                    .setResourceGroup(((ResourceGroupPEntryObject) object).toProto()).build();
        }
        if (object.getClass() == StorageVolumePEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.STORAGE_VOLUME)
                    .setStorageVolume(((StorageVolumePEntryObject) object).toProto()).build();
        }
        if (object.getClass() == WarehousePEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.WAREHOUSE)
                    .setWarehouse(((WarehousePEntryObject) object).toProto()).build();
        }
        if (object.getClass() == PipePEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.PIPE)
                    .setPipe(((PipePEntryObject) object).toProto()).build();
        }
        if (object.getClass() == AIProviderPEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.AI_PROVIDER)
                    .setAiProvider(((AIProviderPEntryObject) object).toProto()).build();
        }
        if (object.getClass() == AIFunctionPEntryObject.class) {
            return builder.setKind(PrivilegeObjectPB.Kind.AI_FUNCTION)
                    .setAiFunction(((AIFunctionPEntryObject) object).toProto()).build();
        }
        if (object instanceof PolicyFCEntryObject) {
            return builder.setKind(PrivilegeObjectPB.Kind.POLICY_COMPATIBLE).build();
        }
        throw new IllegalArgumentException("unsupported privilege object class " + object.getClass().getName());
    }

    public static PEntryObject fromProto(PrivilegeObjectPB pb) {
        ProtoKinds.checkKnown(pb.getKind(), pb.getKindValue());
        switch (pb.getKind()) {
            case DATABASE:
                Preconditions.checkArgument(pb.hasDatabase(), "missing database privilege object");
                return DbPEntryObject.fromProto(pb.getDatabase());
            case TABLE:
                Preconditions.checkArgument(pb.hasTable(), "missing table privilege object");
                return TablePEntryObject.fromProto(pb.getTable());
            case USER:
                Preconditions.checkArgument(pb.hasUser(), "missing user privilege object");
                return UserPEntryObject.fromProto(pb.getUser());
            case RESOURCE:
                Preconditions.checkArgument(pb.hasResource(), "missing resource privilege object");
                return ResourcePEntryObject.fromProto(pb.getResource());
            case VIEW:
                Preconditions.checkArgument(pb.hasTable(), "missing view privilege object");
                return ViewPEntryObject.fromProto(pb.getTable());
            case MATERIALIZED_VIEW:
                Preconditions.checkArgument(pb.hasTable(), "missing materialized_view privilege object");
                return MaterializedViewPEntryObject.fromProto(pb.getTable());
            case GLOBAL_FUNCTION:
                Preconditions.checkArgument(pb.hasGlobalFunction(), "missing global_function privilege object");
                return GlobalFunctionPEntryObject.fromProto(pb.getGlobalFunction());
            case FUNCTION:
                Preconditions.checkArgument(pb.hasFunction(), "missing function privilege object");
                return FunctionPEntryObject.fromProto(pb.getFunction());
            case CATALOG:
                Preconditions.checkArgument(pb.hasCatalog(), "missing catalog privilege object");
                return CatalogPEntryObject.fromProto(pb.getCatalog());
            case RESOURCE_GROUP:
                Preconditions.checkArgument(pb.hasResourceGroup(), "missing resource_group privilege object");
                return ResourceGroupPEntryObject.fromProto(pb.getResourceGroup());
            case STORAGE_VOLUME:
                Preconditions.checkArgument(pb.hasStorageVolume(), "missing storage_volume privilege object");
                return StorageVolumePEntryObject.fromProto(pb.getStorageVolume());
            case WAREHOUSE:
                Preconditions.checkArgument(pb.hasWarehouse(), "missing warehouse privilege object");
                return WarehousePEntryObject.fromProto(pb.getWarehouse());
            case PIPE:
                Preconditions.checkArgument(pb.hasPipe(), "missing pipe privilege object");
                return PipePEntryObject.fromProto(pb.getPipe());
            case AI_PROVIDER:
                return AIProviderPEntryObject.fromProto(pb.getAiProvider());
            case AI_FUNCTION:
                return AIFunctionPEntryObject.fromProto(pb.getAiFunction());
            case POLICY_COMPATIBLE:
                return new PolicyFCEntryObject();
            default:
                throw new IllegalArgumentException("missing privilege object kind");
        }
    }
}
