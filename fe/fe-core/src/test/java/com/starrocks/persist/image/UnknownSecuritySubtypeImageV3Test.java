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

import com.google.gson.JsonObject;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.GroupProvider;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.persist.proto.AnalyzeJobPB;
import com.starrocks.persist.proto.AnalyzeMgrPB;
import com.starrocks.persist.proto.AnalyzeStatusPB;
import com.starrocks.persist.proto.AuthenticationMgrPB;
import com.starrocks.persist.proto.AuthenticationUserAuthenticationEntryPB;
import com.starrocks.persist.proto.AuthenticationUserPropertyEntryPB;
import com.starrocks.persist.proto.GroupProviderPB;
import com.starrocks.persist.proto.SecurityIntegrationPB;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.AnalyzeStatus;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnknownSecuritySubtypeImageV3Test {
    @TempDir
    Path dir;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testAuthenticationMapSubtypesMatchV2(boolean ignore) throws Exception {
        boolean old = Config.metadata_ignore_unknown_subtype;
        Config.metadata_ignore_unknown_subtype = ignore;
        try {
            StatisticsSecurityTestSupport.configureV2GsonReader();
            AuthenticationMgr source = new AuthenticationMgr();
            GroupProvider unix = AuthenticationProtoTest.providers().get(1);
            Map<String, GroupProvider> providers = getField(source, "nameToGroupProviderMap");
            providers.put("file", AuthenticationProtoTest.providers().get(0));
            providers.put("unix", unix);
            SecurityIntegration ldap = AuthenticationProtoTest.integrations().get(1);
            Map<String, SecurityIntegration> integrations = getField(source, "nameToSecurityIntegrationMap");
            integrations.put("jwt", AuthenticationProtoTest.integrations().get(0));
            integrations.put("ldap", ldap);
            JsonObject future = GsonUtils.GSON.toJsonTree(source).getAsJsonObject();
            future.getAsJsonObject("gp").getAsJsonObject("file").addProperty("clazz", "FutureGroupProvider");
            future.getAsJsonObject("sim").getAsJsonObject("jwt").addProperty("clazz", "FutureSecurityIntegration");
            StatisticsSecurityTestSupport.SaveV2 save = writer -> {
                SRMetaBlockWriter block = writer.getBlockWriter(SRMetaBlockID.AUTHENTICATION_MGR, 4);
                block.writeJson(future);
                block.writeInt(1);
                block.writeJson(UserIdentity.ROOT);
                block.writeJson(source.getUserToAuthenticationInfo().get(UserIdentity.ROOT));
                block.close();
            };
            Path file = StatisticsSecurityTestSupport.sections(dir,
                    AuthenticationMgrPB.getDefaultInstance(), List.of(
                            GroupProviderPB.newBuilder().setKindValue(999).setName("file").build(),
                            unix.toProto(),
                            SecurityIntegrationPB.newBuilder().setKindValue(999).setName("jwt").build(),
                            ldap.toProto(),
                            AuthenticationUserPropertyEntryPB.newBuilder()
                                    .setUserName("root").setProperty(source.getUserProperty("root").toProto()).build(),
                            AuthenticationUserAuthenticationEntryPB.newBuilder()
                                            .setUser(UserIdentityUtils.toProto(UserIdentity.ROOT))
                                            .setAuthentication(source.getUserToAuthenticationInfo()
                                                    .get(UserIdentity.ROOT).toProto()).build()));
            AuthenticationMgr v2 = new AuthenticationMgr();
            AuthenticationMgr loaded = new AuthenticationMgr();
            if (!ignore) {
                assertThrows(SubtypeNotFoundException.class, () -> StatisticsSecurityTestSupport.v2(save, v2::loadV2));
                assertThrows(ImageFormatException.class, () -> LoadImageTestSupport.load(file, loaded.getImageV3Loaders()));
                return;
            }
            StatisticsSecurityTestSupport.v2(save, v2::loadV2);
            LoadImageTestSupport.load(file, loaded.getImageV3Loaders());
            AuthenticationMgrImageV3Test.equivalent(v2, loaded);
            assertNull(loaded.getGroupProvider("file"));
            assertNull(loaded.getSecurityIntegration("jwt"));
            assertNotNull(loaded.getGroupProvider("unix"));
            assertNotNull(loaded.getSecurityIntegration("ldap"));
        } finally {
            Config.metadata_ignore_unknown_subtype = old;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testAnalyzeCollectionSubtypesMatchV2(boolean ignore) throws Exception {
        boolean old = Config.metadata_ignore_unknown_subtype;
        Config.metadata_ignore_unknown_subtype = ignore;
        try {
            StatisticsSecurityTestSupport.configureV2GsonReader();
            AnalyzeJob first = AnalyzeJobProtoTest.job(false, 1);
            AnalyzeJob last = AnalyzeJobProtoTest.job(true, 3);
            JsonObject futureJob = GsonUtils.GSON.toJsonTree(AnalyzeJobProtoTest.job(false, 2), AnalyzeJob.class)
                    .getAsJsonObject();
            futureJob.addProperty("clazz", "FutureAnalyzeJob");
            JsonObject futureStatus = GsonUtils.GSON.toJsonTree(AnalyzeStatusProtoTest.status(false, 4), AnalyzeStatus.class)
                    .getAsJsonObject();
            futureStatus.addProperty("clazz", "FutureAnalyzeStatus");
            StatisticsSecurityTestSupport.SaveV2 save = writer -> {
                SRMetaBlockWriter block = writer.getBlockWriter(SRMetaBlockID.ANALYZE_MGR, 11);
                block.writeInt(3);
                block.writeJson(first);
                block.writeJson(futureJob);
                block.writeJson(last);
                block.writeInt(1);
                block.writeJson(futureStatus);
                for (int i = 0; i < 5; i++) {
                    block.writeInt(0);
                }
                block.close();
            };
            Path file = StatisticsSecurityTestSupport.sections(dir, AnalyzeMgrPB.getDefaultInstance(), List.of(
                    first.toProto(),
                    AnalyzeJobPB.newBuilder().setKindValue(999).build(),
                    last.toProto(),
                    AnalyzeStatusPB.newBuilder().setKindValue(999).build()));
            AnalyzeMgr v2 = new AnalyzeMgr();
            AnalyzeMgr loaded = new AnalyzeMgr();
            if (!ignore) {
                assertThrows(SubtypeNotFoundException.class, () -> StatisticsSecurityTestSupport.v2(save, v2::load));
                assertThrows(ImageFormatException.class, () -> LoadImageTestSupport.load(file, loaded.getImageV3Loaders()));
                return;
            }
            StatisticsSecurityTestSupport.v2(save, v2::load);
            LoadImageTestSupport.load(file, loaded.getImageV3Loaders());
            AnalyzeMgrImageV3Test.equivalent(v2, loaded);
            Map<Long, AnalyzeJob> jobs = getField(loaded, "analyzeJobMap");
            assertEquals(Set.of(1L, 3L), jobs.keySet());
            assertTrue(loaded.getAnalyzeStatusMap().isEmpty());
        } finally {
            Config.metadata_ignore_unknown_subtype = old;
        }
    }
}
