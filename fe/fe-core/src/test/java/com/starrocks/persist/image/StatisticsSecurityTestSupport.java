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

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.DefaultGsonBuilderFactory;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.AnalyzeJobPB;
import com.starrocks.persist.proto.AnalyzeMgrPB;
import com.starrocks.persist.proto.AnalyzeStatusPB;
import com.starrocks.persist.proto.AuthenticationMgrPB;
import com.starrocks.persist.proto.AuthenticationUserAuthenticationEntryPB;
import com.starrocks.persist.proto.AuthenticationUserPropertyEntryPB;
import com.starrocks.persist.proto.AuthorizationGroupRolesEntryPB;
import com.starrocks.persist.proto.AuthorizationMgrPB;
import com.starrocks.persist.proto.AuthorizationRoleHeaderEntryPB;
import com.starrocks.persist.proto.AuthorizationRoleNameIdEntryPB;
import com.starrocks.persist.proto.AuthorizationRolePrivilegeEntryPB;
import com.starrocks.persist.proto.AuthorizationUserHeaderEntryPB;
import com.starrocks.persist.proto.AuthorizationUserPrivilegeEntryPB;
import com.starrocks.persist.proto.BasicStatsMetaPB;
import com.starrocks.persist.proto.ExternalBasicStatsMetaPB;
import com.starrocks.persist.proto.ExternalHistogramStatsMetaPB;
import com.starrocks.persist.proto.GroupProviderPB;
import com.starrocks.persist.proto.HistogramStatsMetaPB;
import com.starrocks.persist.proto.MultiColumnStatsMetaPB;
import com.starrocks.persist.proto.SecurityIntegrationPB;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;

import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class StatisticsSecurityTestSupport {
    private StatisticsSecurityTestSupport() {
    }

    @FunctionalInterface
    interface SaveV2 {
        void save(ImageWriter writer) throws Exception;
    }

    @FunctionalInterface
    interface LoadV2 {
        void load(SRMetaBlockReader reader) throws Exception;
    }

    static void v2(SaveV2 save, LoadV2 load) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        save.save(image.getImageWriter());
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        load.load(reader);
        reader.close();
    }

    static Path sections(Path dir, Message manager,
                         List<? extends Message> entries) throws Exception {
        Map<Class<?>, SectionId> sections = new LinkedHashMap<>();
        if (manager instanceof AnalyzeMgrPB) {
            sections.put(AnalyzeJobPB.class, SectionId.ANALYZE_JOB);
            sections.put(AnalyzeStatusPB.class, SectionId.ANALYZE_STATUS);
            sections.put(BasicStatsMetaPB.class, SectionId.ANALYZE_BASIC_STATS);
            sections.put(HistogramStatsMetaPB.class, SectionId.ANALYZE_HISTOGRAM_STATS);
            sections.put(ExternalBasicStatsMetaPB.class, SectionId.ANALYZE_EXTERNAL_BASIC_STATS);
            sections.put(ExternalHistogramStatsMetaPB.class, SectionId.ANALYZE_EXTERNAL_HISTOGRAM_STATS);
            sections.put(MultiColumnStatsMetaPB.class, SectionId.ANALYZE_MULTI_COLUMN_STATS);
        } else if (manager instanceof AuthenticationMgrPB) {
            sections.put(AuthenticationUserPropertyEntryPB.class, SectionId.AUTHENTICATION_USER_PROPERTY);
            sections.put(GroupProviderPB.class, SectionId.AUTHENTICATION_GROUP_PROVIDER);
            sections.put(SecurityIntegrationPB.class, SectionId.AUTHENTICATION_SECURITY_INTEGRATION);
            sections.put(AuthenticationUserAuthenticationEntryPB.class, SectionId.AUTHENTICATION_USER);
        } else if (manager instanceof AuthorizationMgrPB) {
            sections.put(AuthorizationRoleNameIdEntryPB.class, SectionId.AUTHORIZATION_ROLE_NAME);
            sections.put(AuthorizationGroupRolesEntryPB.class, SectionId.AUTHORIZATION_GROUP_ROLES);
            sections.put(AuthorizationUserHeaderEntryPB.class, SectionId.AUTHORIZATION_USER_HEADER);
            sections.put(AuthorizationUserPrivilegeEntryPB.class, SectionId.AUTHORIZATION_USER_PRIVILEGE);
            sections.put(AuthorizationRoleHeaderEntryPB.class, SectionId.AUTHORIZATION_ROLE_HEADER);
            sections.put(AuthorizationRolePrivilegeEntryPB.class, SectionId.AUTHORIZATION_ROLE_PRIVILEGE);
        } else {
            throw new IllegalArgumentException("unknown test manager " + manager.getClass());
        }
        for (Message entry : entries) {
            assertTrue(sections.containsKey(entry.getClass()));
        }
        return LoadImageTestSupport.save(dir, writer -> {
            boolean first = true;
            for (Map.Entry<Class<?>, SectionId> section : sections.entrySet()) {
                try (SectionWriter out = writer.beginSection(section.getValue())) {
                    if (first) {
                        out.writeManager(manager);
                        first = false;
                    }
                    for (Message entry : entries) {
                        if (entry.getClass() == section.getKey()) {
                            out.writeEntry(entry);
                        }
                    }
                }
            }
        });
    }

    static <T> T gson(T value, Class<T> type) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(value, type), type);
    }

    static <K, V> void assertMap(Map<K, V> expected, Map<K, V> actual) {
        assertEquals(expected.keySet(), actual.keySet());
        expected.forEach((key, value) -> GsonEquivalence.assertEquivalent(value, actual.get(key)));
    }

    static void assertMaps(Object expected, Object actual, String... fields) {
        for (String field : fields) {
            assertMap(getField(expected, field), getField(actual, field));
        }
    }

    static UnknownFieldSet unknownFields() {
        return UnknownFieldSet.newBuilder().addField(1000,
                UnknownFieldSet.Field.newBuilder().addVarint(91).build()).build();
    }

    // This config is fixed at startup. Build its adapters afresh so tests do not depend
    // on which earlier test first initialized the static GsonUtils.GSON instance.
    static void configureV2GsonReader() {
        Gson configured = new DefaultGsonBuilderFactory().create().create();
        new MockUp<Gson>() {
            @Mock
            public <T> T fromJson(Invocation invocation, JsonReader reader, Type type) {
                if (invocation.getInvokedInstance() == GsonUtils.GSON) {
                    return configured.fromJson(reader, type);
                }
                return invocation.proceed(reader, type);
            }
        };
    }

}
