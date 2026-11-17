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

import com.starrocks.authorization.ActionSet;
import com.starrocks.authorization.FunctionPEntryObject;
import com.starrocks.authorization.GlobalFunctionPEntryObject;
import com.starrocks.authorization.PEntryObject;
import com.starrocks.authorization.PolicyFCEntryObject;
import com.starrocks.authorization.PrivilegeEntry;
import com.starrocks.authorization.PrivilegeObjectProtoConverter;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.RolePrivilegeCollectionV2;
import com.starrocks.authorization.RolePrivilegeCollectionV2.RoleFlags;
import com.starrocks.authorization.TablePEntryObject;
import com.starrocks.authorization.UserPrivilegeCollectionV2;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.ActionSetPB;
import com.starrocks.persist.proto.PrivilegeEntryPB;
import com.starrocks.persist.proto.PrivilegeObjectPB;
import com.starrocks.persist.proto.RolePrivilegeHeaderPB;
import com.starrocks.persist.proto.UserPrivilegeHeaderPB;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PrivilegeProtoTest {
    static PEntryObject object(String kind, String fields) {
        return GsonUtils.GSON.fromJson("{\"clazz\":\"" + kind + "\"" + (fields.isEmpty() ? "" : "," + fields) + "}",
                PEntryObject.class);
    }

    static TablePEntryObject table(long id) {
        return (TablePEntryObject) object("TablePEntryObject", "\"ci\":0,\"d\":\"10\",\"t\":\"" + id + "\"");
    }

    static List<PEntryObject> objects() {
        return List.of(object("DbPEntryObject", "\"ci\":7,\"i\":\"db.uuid\""),
                object("TablePEntryObject", "\"ci\":7,\"d\":\"db.uuid\",\"t\":\"hive.db.table.123\""),
                object("UserPEntryObject", "\"u\":{\"user\":\"alice\",\"host\":\"%\",\"isDomain\":false}"),
                object("ResourcePEntryObject", "\"n\":\"resource\""),
                object("ViewPEntryObject", "\"ci\":7,\"d\":\"db.uuid\",\"t\":\"hive.db.view.123\""),
                object("MaterializedViewPEntryObject", "\"ci\":7,\"d\":\"db.uuid\",\"t\":\"hive.db.mv.123\""),
                object("GlobalFunctionPEntryObject", "\"f\":\"legacy_function(INT)\""),
                object("FunctionPEntryObject", "\"d\":10,\"functionId\":20"),
                object("CatalogPEntryObject", "\"i\":7"),
                object("ResourceGroupPEntryObject", "\"i\":11"),
                object("StorageVolumePEntryObject", "\"id\":\"volume.uuid\""),
                object("WarehousePEntryObject", "\"i\":17"),
                object("PipePEntryObject", "\"database\":\"10\",\"id\":21"),
                object("AIProviderPEntryObject", "\"id\":\"provider-uuid\""),
                object("AIFunctionPEntryObject", "\"name\":\"ai_query\""));
    }

    @Test
    public void testAllPrivilegeObjects() throws Exception {
        assertEquals(15, objects().size());
        for (PEntryObject source : objects()) {
            PrivilegeObjectPB pb = PrivilegeObjectPB.parseFrom(
                    assertInstances(PrivilegeObjectProtoConverter.toProto(source)).toByteArray());
            PEntryObject loaded = PrivilegeObjectProtoConverter.fromProto(pb);
            assertEquals(source.getClass(), loaded.getClass());
            GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, PEntryObject.class), loaded);
            assertEquals(source, loaded);
            assertEquals(source.isFuzzyMatching(), loaded.isFuzzyMatching());
            assertTrue(source.match(loaded));
            PEntryObject v2 = StatisticsSecurityTestSupport.gson(source, PEntryObject.class);
            assertEquals(v2.compareTo(source), loaded.compareTo(source), source.getClass().getSimpleName());
        }
    }

    @Test
    public void testViewAndMaterializedViewKeepSubtypeAndCatalog() {
        for (String kind : List.of("TablePEntryObject", "ViewPEntryObject", "MaterializedViewPEntryObject")) {
            TablePEntryObject source = (TablePEntryObject) object(kind, "\"ci\":23,\"d\":\"db\",\"t\":\"table\"");
            TablePEntryObject loaded = (TablePEntryObject) PrivilegeObjectProtoConverter.fromProto(
                    PrivilegeObjectProtoConverter.toProto(source));
            assertEquals(source.getClass(), loaded.getClass());
            assertEquals(23, loaded.getCatalogId());
            assertEquals("db", loaded.getDatabaseUUID());
            assertEquals("table", loaded.getTableUUID());
        }
    }

    @Test
    public void testNullResourceAndUserMeanAll() {
        for (String kind : List.of("ResourcePEntryObject", "UserPEntryObject")) {
            PEntryObject source = object(kind, "");
            PEntryObject loaded = PrivilegeObjectProtoConverter.fromProto(PrivilegeObjectProtoConverter.toProto(source));
            assertTrue(loaded.isFuzzyMatching());
            GsonEquivalence.assertEquivalent(source, loaded);
        }
        PEntryObject resource = object("ResourcePEntryObject", "\"n\":\"specific\"");
        PEntryObject allResources = PrivilegeObjectProtoConverter.fromProto(
                PrivilegeObjectProtoConverter.toProto(object("ResourcePEntryObject", "")));
        assertTrue(resource.match(allResources));
        assertFalse(allResources.match(resource));
    }

    @Test
    public void testAllObjectSentinels() {
        List<PEntryObject> allObjects = List.of(object("DbPEntryObject", "\"ci\":-1,\"i\":\"ALL_DATABASES_UUID\""),
                object("TablePEntryObject", "\"ci\":-1,\"d\":\"ALL_DATABASES_UUID\",\"t\":\"ALL_TABLES_UUID\""),
                object("FunctionPEntryObject", "\"d\":-2,\"functionId\":-3"),
                object("GlobalFunctionPEntryObject", "\"f\":\"AGFS\""),
                object("CatalogPEntryObject", "\"i\":-1"), object("ResourceGroupPEntryObject", "\"i\":-1"),
                object("StorageVolumePEntryObject", "\"id\":\"ALL_STORAGE_VOLUMES_ID\""),
                object("WarehousePEntryObject", "\"i\":-1"),
                object("PipePEntryObject", "\"database\":\"ALL_DATABASES_UUID\",\"id\":-5"));
        for (PEntryObject source : allObjects) {
            PEntryObject loaded = PrivilegeObjectProtoConverter.fromProto(PrivilegeObjectProtoConverter.toProto(source));
            GsonEquivalence.assertEquivalent(source, loaded);
            assertTrue(loaded.isFuzzyMatching());
        }
    }

    @Test
    public void testNullableFunctionFields() {
        FunctionPEntryObject function = (FunctionPEntryObject) object("FunctionPEntryObject", "\"d\":10");
        FunctionPEntryObject loaded = (FunctionPEntryObject) PrivilegeObjectProtoConverter.fromProto(
                PrivilegeObjectProtoConverter.toProto(function));
        assertEquals(10, loaded.getDatabaseId());
        assertNull(loaded.getFunctionId());
        GlobalFunctionPEntryObject global = (GlobalFunctionPEntryObject) object("GlobalFunctionPEntryObject", "");
        GlobalFunctionPEntryObject globalLoaded = (GlobalFunctionPEntryObject) PrivilegeObjectProtoConverter.fromProto(
                PrivilegeObjectProtoConverter.toProto(global));
        assertNull(globalLoaded.getFunctionSig());
    }

    @Test
    public void testActionSetKeepsAllBits() throws Exception {
        for (long bits : List.of(0L, 1L, Long.MIN_VALUE, Long.MAX_VALUE, -1L, 0x456789abcdef1234L)) {
            ActionSet source = GsonUtils.GSON.fromJson("{\"b\":" + bits + "}", ActionSet.class);
            ActionSet loaded = ActionSet.fromProto(ActionSetPB.parseFrom(source.toProto().toByteArray()));
            assertEquals(bits, (long) getField(loaded, "bitSet"));
            GsonEquivalence.assertEquivalent(source, loaded);
        }
        ActionSet source = new ActionSet(List.of(PrivilegeType.SELECT, PrivilegeType.INSERT));
        ActionSet loaded = ActionSet.fromProto(source.toProto());
        assertTrue(loaded.contains(PrivilegeType.SELECT));
        assertTrue(loaded.contains(PrivilegeType.INSERT));
        assertFalse(loaded.contains(PrivilegeType.DELETE));
        loaded.remove(new ActionSet(List.of(PrivilegeType.SELECT)));
        assertTrue(source.contains(PrivilegeType.SELECT));
    }

    @Test
    public void testSystemNullObjectAndGrantOption() throws Exception {
        PrivilegeEntry source = new PrivilegeEntry(new ActionSet(List.of(PrivilegeType.OPERATE)), null, true);
        PrivilegeEntry loaded = PrivilegeEntry.fromProto(PrivilegeEntryPB.parseFrom(source.toProto().toByteArray()));
        assertNull(loaded.getObject());
        assertTrue(loaded.isWithGrantOption());
        assertTrue(loaded.getActionSet().contains(PrivilegeType.OPERATE));
        GsonEquivalence.assertEquivalent(source, loaded);
    }

    @Test
    public void testEveryObjectInPrivilegeEntry() throws Exception {
        for (PEntryObject object : objects()) {
            for (boolean grantOption : List.of(false, true)) {
                PrivilegeEntry source = new PrivilegeEntry(new ActionSet(List.of(PrivilegeType.SELECT)), object, grantOption);
                PrivilegeEntry loaded = PrivilegeEntry.fromProto(PrivilegeEntryPB.parseFrom(source.toProto().toByteArray()));
                GsonEquivalence.assertEquivalent(source, loaded);
                assertEquals(grantOption, loaded.isWithGrantOption());
            }
        }
    }

    @Test
    public void testPolicyCompatibilityPlaceholder() {
        PrivilegeEntry source = new PrivilegeEntry(new ActionSet(List.of(PrivilegeType.SELECT)),
                new PolicyFCEntryObject(), false);
        PrivilegeEntry loaded = PrivilegeEntry.fromProto(source.toProto());
        assertInstanceOf(PolicyFCEntryObject.class, loaded.getObject());
        assertEquals(PrivilegeObjectPB.Kind.POLICY_COMPATIBLE, source.toProto().getObject().getKind());
    }

    @Test
    public void testUserMetadataHeader() {
        UserPrivilegeCollectionV2 source = new UserPrivilegeCollectionV2();
        source.grantRoles(Set.of(1L, 2L));
        source.setDefaultRoleIds(new HashSet<>(Set.of(2L)));
        UserPrivilegeCollectionV2 loaded = UserPrivilegeCollectionV2.fromHeaderProto(source.toHeaderProto());
        GsonEquivalence.assertEquivalent(source, loaded);
        assertEquals(Set.of(1L, 2L), loaded.getAllRoles());
        assertEquals(Set.of(2L), loaded.getDefaultRoleIds());
        loaded.getAllRoles().clear();
        loaded.getDefaultRoleIds().clear();
        assertEquals(2, source.getAllRoles().size());
        assertEquals(1, source.getDefaultRoleIds().size());
    }

    @Test
    public void testRoleMetadataHeader() throws Exception {
        RolePrivilegeCollectionV2 source = new RolePrivilegeCollectionV2("role", "comment",
                RoleFlags.MUTABLE, RoleFlags.REMOVABLE);
        source.addParentRole(1);
        source.addSubRole(2);
        RolePrivilegeCollectionV2 loaded = RolePrivilegeCollectionV2.fromHeaderProto(source.toHeaderProto());
        GsonEquivalence.assertEquivalent(source, loaded);
        assertEquals("role", loaded.getName());
        assertEquals("comment", loaded.getComment());
        assertTrue(loaded.isMutable());
        assertTrue(loaded.isRemovable());
        assertEquals(Set.of(1L), loaded.getParentRoleIds());
        assertEquals(Set.of(2L), loaded.getSubRoleIds());
        loaded.removeParentRole(1);
        loaded.removeSubRole(2);
        assertEquals(Set.of(1L), source.getParentRoleIds());
        assertEquals(Set.of(2L), source.getSubRoleIds());
    }

    @Test
    public void testHeaderDefaultsFollowGson() {
        GsonEquivalence.assertEquivalent(GsonUtils.GSON.fromJson("{}", UserPrivilegeCollectionV2.class),
                UserPrivilegeCollectionV2.fromHeaderProto(UserPrivilegeHeaderPB.getDefaultInstance()));
        GsonEquivalence.assertEquivalent(GsonUtils.GSON.fromJson("{}", RolePrivilegeCollectionV2.class),
                RolePrivilegeCollectionV2.fromHeaderProto(RolePrivilegeHeaderPB.getDefaultInstance()));
        UserPrivilegeCollectionV2 user = new UserPrivilegeCollectionV2();
        setField(user, "roleIds", null);
        setField(user, "defaultRoleIds", null);
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(user, UserPrivilegeCollectionV2.class),
                UserPrivilegeCollectionV2.fromHeaderProto(user.toHeaderProto()));
    }

    @Test
    public void testUnknownAdditionalFields() {
        for (PEntryObject source : objects()) {
            PrivilegeObjectPB pb = PrivilegeObjectProtoConverter.toProto(source).toBuilder()
                    .setUnknownFields(StatisticsSecurityTestSupport.unknownFields()).build();
            GsonEquivalence.assertEquivalent(source, PrivilegeObjectProtoConverter.fromProto(pb));
        }
    }

    @Test
    public void testUnknownSubtypeIsNotSystemNull() {
        PrivilegeEntryPB pb = PrivilegeEntryPB.newBuilder().setActionSet(ActionSetPB.newBuilder().setBitSet(-1))
                .setObject(PrivilegeObjectPB.newBuilder().setKindValue(999)).build();
        assertThrows(SubtypeNotFoundException.class, () -> PrivilegeEntry.fromProto(pb));
        assertNull(PrivilegeEntry.fromProto(pb.toBuilder().clearObject().build()).getObject());
    }


}
