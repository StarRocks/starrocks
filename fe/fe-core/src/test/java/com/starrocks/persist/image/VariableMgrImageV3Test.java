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

import com.starrocks.common.Pair;
import com.starrocks.persist.GlobalVarPersistInfo;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.VariableMgrPB;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.sql.ast.SetType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The VARIABLE_MGR section: a VariableMgr saved as a v3 image and loaded back shows the same global values as the
 * same manager put through the v2 image.
 */
public class VariableMgrImageV3Test {
    /**
     * Non-default values of every persisted type (int, long, boolean, String, double session defaults), plus the
     * global-only variables: a GLOBAL one and the read-only enable_table_name_case_insensitive that v2 persists.
     */
    private static final String CHANGED = "{\"query_timeout\": 100, \"exec_mem_limit\": 8589934592,"
            + " \"enable_profile\": true, \"time_zone\": \"Asia/Tokyo\", \"insert_max_filter_ratio\": 0.25,"
            + " \"query_cache_size\": 2097152, \"init_connect\": \"SET NAMES utf8\","
            + " \"enable_table_name_case_insensitive\": true}";
    /** The global-only variables back to other values: GlobalVariable's fields are static, shared by every manager. */
    private static final String GLOBALS_RESET = "{\"query_cache_size\": 1048576, \"init_connect\": \"\","
            + " \"enable_table_name_case_insensitive\": false}";

    @TempDir
    Path tempDir;

    private static VariableMgr newManagerWith(String json) throws Exception {
        VariableMgr mgr = new VariableMgr();
        GlobalVarPersistInfo info = new GlobalVarPersistInfo();
        info.setPersistJsonString(json);
        mgr.replayGlobalVariableV2(info);
        return mgr;
    }

    /** SHOW GLOBAL VARIABLES: every variable with its global value, as name-value rows. */
    private static List<String> globalRows(VariableMgr mgr) {
        List<String> rows = new ArrayList<>();
        for (List<String> row : mgr.dump(SetType.GLOBAL, null, null)) {
            rows.add(String.join("\t", row));
        }
        Collections.sort(rows);
        return rows;
    }

    private static int countPersistedSessionVariables() {
        int count = 0;
        for (Field field : SessionVariable.class.getDeclaredFields()) {
            VariableMgr.VarAttr attr = field.getAnnotation(VariableMgr.VarAttr.class);
            if (attr != null && (attr.flag() & VariableMgr.SESSION_ONLY) != VariableMgr.SESSION_ONLY) {
                count++;
            }
        }
        return count;
    }

    private static int countPersistedGlobalVariables() {
        int count = 0;
        for (Field field : GlobalVariable.class.getDeclaredFields()) {
            VariableMgr.VarAttr attr = field.getAnnotation(VariableMgr.VarAttr.class);
            if (attr != null && ((attr.flag() & VariableMgr.GLOBAL) == VariableMgr.GLOBAL
                    || attr.name().equalsIgnoreCase(GlobalVariable.ENABLE_TABLE_NAME_CASE_INSENSITIVE))) {
                count++;
            }
        }
        return count;
    }

    @Test
    public void testV3ImageMatchesV2() throws Exception {
        VariableMgr live = newManagerWith(CHANGED);
        List<String> expected = globalRows(live);
        Assertions.assertTrue(expected.contains("query_timeout\t100"), expected.toString());
        Assertions.assertTrue(expected.contains("query_cache_size\t2097152"), expected.toString());

        // reference: the v2 image round trip, the global-only variables reset first so that the load must write them
        UtFrameUtils.PseudoImage v2Image = new UtFrameUtils.PseudoImage();
        live.save(v2Image.getImageWriter());
        newManagerWith(GLOBALS_RESET);
        Assertions.assertNotEquals(expected, globalRows(new VariableMgr()));
        VariableMgr v2Loaded = new VariableMgr();
        v2Loaded.load(v2Image.getMetaBlockReader());
        Assertions.assertEquals(expected, globalRows(v2Loaded));

        // the v3 image round trip
        Path file = tempDir.resolve("variable_mgr.image");
        try (OutputStream os = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(os, 1L)) {
            live.save(writer);
            writer.finish();
        }
        newManagerWith(GLOBALS_RESET);
        Assertions.assertNotEquals(expected, globalRows(new VariableMgr()));
        VariableMgr v3Loaded = new VariableMgr();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            // one section, the manager message only
            List<IndexEntryPB> sections = reader.sections();
            Assertions.assertEquals(1, sections.size());
            IndexEntryPB section = sections.get(0);
            Assertions.assertEquals(SectionId.VARIABLE_MGR.getValue(), section.getType());
            Assertions.assertEquals(0, section.getNumEntries());
            // the manager message carries exactly the variables v2 writes
            reader.load(List.of(Pair.create(SectionId.VARIABLE_MGR, r -> {
                VariableMgrPB pb = r.readManager(VariableMgrPB.parser());
                Assertions.assertEquals(countPersistedSessionVariables(), pb.getSessionVariableDefaultsCount());
                Assertions.assertEquals(countPersistedGlobalVariables(), pb.getGlobalVariablesCount());
                Assertions.assertEquals("100", pb.getSessionVariableDefaultsMap().get("query_timeout"));
                Assertions.assertEquals("0.25", pb.getSessionVariableDefaultsMap().get("insert_max_filter_ratio"));
                Assertions.assertEquals("2097152", pb.getGlobalVariablesMap().get("query_cache_size"));
                Assertions.assertEquals("true", pb.getGlobalVariablesMap().get("enable_table_name_case_insensitive"));
            })));
        }
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            reader.load(v3Loaded.getImageV3Loaders());
        }
        Assertions.assertEquals(expected, globalRows(v3Loaded));
        Assertions.assertEquals(100, v3Loaded.getDefaultSessionVariable().getQueryTimeoutS());
        Assertions.assertEquals("Asia/Tokyo", v3Loaded.getDefaultSessionVariable().getTimeZone());
        Assertions.assertTrue(GlobalVariable.enableTableNameCaseInsensitive);
    }
}
