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

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Pair;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.statistic.AnalyzeMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StatisticsSecurityImageV3Test {
    @TempDir
    Path dir;

    @BeforeEach
    public void setUp() {
        AuthorizationMgrImageV3Test.global();
        AuthenticationMgrImageV3Test.mockProviderInit(new HashMap<>());
    }

    @Test
    public void testAllFourSectionsInV2SaveOrder() throws Exception {
        AnalyzeMgr analyze = AnalyzeMgrImageV3Test.populated();
        ResourceGroupMgr groups = ResourceGroupMgrImageV3Test.populated();
        AuthenticationMgr authentication = AuthenticationMgrImageV3Test.populated();
        AuthorizationMgr authorization = AuthorizationMgrImageV3Test.populated();
        AnalyzeMgr analyzeV2 = new AnalyzeMgr();
        ResourceGroupMgr groupsV2 = new ResourceGroupMgr();
        AuthenticationMgr authenticationV2 = new AuthenticationMgr();
        AuthorizationMgr authorizationV2 = AuthorizationMgrImageV3Test.manager();
        StatisticsSecurityTestSupport.v2(analyze::save, analyzeV2::load);
        StatisticsSecurityTestSupport.v2(groups::save, groupsV2::load);
        StatisticsSecurityTestSupport.v2(authentication::saveV2, authenticationV2::loadV2);
        StatisticsSecurityTestSupport.v2(authorization::saveV2, authorizationV2::loadV2);
        Path file = LoadImageTestSupport.save(dir, writer -> {
            analyze.save(writer);
            groups.save(writer);
            authentication.save(writer);
            authorization.save(writer);
        });
        AnalyzeMgr loadedAnalyze = new AnalyzeMgr();
        ResourceGroupMgr loadedGroups = new ResourceGroupMgr();
        AuthenticationMgr loadedAuthentication = new AuthenticationMgr();
        AuthorizationMgr loadedAuthorization = AuthorizationMgrImageV3Test.manager();
        List<Pair<SectionId, SectionLoader>> loaders = new ArrayList<>();
        loaders.addAll(loadedAnalyze.getImageV3Loaders());
        loaders.addAll(loadedGroups.getImageV3Loaders());
        loaders.addAll(loadedAuthentication.getImageV3Loaders());
        loaders.addAll(loadedAuthorization.getImageV3Loaders());
        List<IndexEntryPB> sections = LoadImageTestSupport.load(file, loaders);
        assertEquals(List.of(160, 161, 162, 163, 164, 165, 166, 170, 180, 181, 182, 183,
                        190, 191, 192, 193, 194, 195),
                sections.stream().map(IndexEntryPB::getType).collect(Collectors.toList()));
        AnalyzeMgrImageV3Test.equivalent(analyzeV2, loadedAnalyze);
        StatisticsSecurityTestSupport.assertMaps(groupsV2, loadedGroups,
                "resourceGroupMap", "id2ResourceGroupMap", "classifierMap");
        AuthenticationMgrImageV3Test.equivalent(authenticationV2, loadedAuthentication);
        AuthorizationMgrImageV3Test.equivalent(authorizationV2, loadedAuthorization);
        UserIdentity matched = loadedAuthentication.getBestMatchedUserIdentity("alice", "10.0.0.3").getKey();
        assertEquals(AuthorizationMgrImageV3Test.ALICE, matched);
        assertTrue(loadedAuthorization.getUserPrivilegeCollection(matched)
                .check(ObjectType.TABLE, PrivilegeType.SELECT, PrivilegeProtoTest.table(20)));
        assertFalse(loadedAuthorization.getUserPrivilegeCollection(matched)
                .check(ObjectType.TABLE, PrivilegeType.SELECT, PrivilegeProtoTest.table(21)));

    }

    @Test
    public void testEmptyAndDefaultManagers() throws Exception {
        AnalyzeMgr analyze = new AnalyzeMgr();
        ResourceGroupMgr groups = new ResourceGroupMgr();
        AuthenticationMgr authentication = new AuthenticationMgr();
        AuthorizationMgr authorization = AuthorizationMgrImageV3Test.manager();
        Path file = LoadImageTestSupport.save(dir, writer -> {
            analyze.save(writer);
            groups.save(writer);
            authentication.save(writer);
            authorization.save(writer);
        });
        AnalyzeMgr loadedAnalyze = new AnalyzeMgr();
        ResourceGroupMgr loadedGroups = new ResourceGroupMgr();
        AuthenticationMgr loadedAuthentication = new AuthenticationMgr();
        AuthorizationMgr loadedAuthorization = AuthorizationMgrImageV3Test.manager();
        List<Pair<SectionId, SectionLoader>> loaders = new ArrayList<>();
        loaders.addAll(loadedAnalyze.getImageV3Loaders());
        loaders.addAll(loadedGroups.getImageV3Loaders());
        loaders.addAll(loadedAuthentication.getImageV3Loaders());
        loaders.addAll(loadedAuthorization.getImageV3Loaders());
        List<IndexEntryPB> sections = LoadImageTestSupport.load(file, loaders);
        assertEquals(18, sections.size());
        assertTrue(sections.subList(0, 8).stream().allMatch(section -> section.getNumEntries() == 0));
        assertTrue(loadedAuthentication.doesUserExist(UserIdentity.ROOT));
        assertTrue(loadedAuthorization.getUserPrivilegeCollection(UserIdentity.ROOT).getAllRoles()
                .contains(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        AnalyzeMgrImageV3Test.equivalent(analyze, loadedAnalyze);
        AuthenticationMgrImageV3Test.equivalent(authentication, loadedAuthentication);
        AuthorizationMgrImageV3Test.equivalent(authorization, loadedAuthorization);

    }

    @Test
    public void testLoadingOnlyRequestedSections() throws Exception {
        Path file = LoadImageTestSupport.save(dir, writer -> {
            AnalyzeMgrImageV3Test.populated().save(writer);
            ResourceGroupMgrImageV3Test.populated().save(writer);
            AuthenticationMgrImageV3Test.populated().save(writer);
            AuthorizationMgrImageV3Test.manager().save(writer);
        });
        AnalyzeMgr loadedAnalyze = new AnalyzeMgr();
        AuthenticationMgr loadedAuthentication = new AuthenticationMgr();
        List<Pair<SectionId, SectionLoader>> loaders = new ArrayList<>();
        loaders.addAll(loadedAnalyze.getImageV3Loaders());
        loaders.addAll(loadedAuthentication.getImageV3Loaders());
        List<IndexEntryPB> sections = LoadImageTestSupport.load(file, loaders);
        assertEquals(18, sections.size());
        assertEquals(1, loadedAnalyze.getBasicStatsMetaMap().size());
        assertEquals(4, loadedAuthentication.getUserToAuthenticationInfo().size());
        assertTrue(loadedAuthentication.isLoaded());
    }
}
