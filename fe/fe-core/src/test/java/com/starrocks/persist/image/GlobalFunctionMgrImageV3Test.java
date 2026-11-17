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

import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.GlobalFunctionMgr;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.SqlFunction;
import com.starrocks.catalog.TableFunction;
import com.starrocks.common.Config;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.FunctionPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GlobalFunctionMgrImageV3Test {
    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        LoadImageTestSupport.global();
    }

    private static FunctionName name(String value) {
        FunctionName name = new FunctionName(null, value);
        name.setAsGlobalFunction();
        return name;
    }

    static ScalarFunction scalar(String name, Type arg, long id) {
        ScalarFunction function = ScalarFunction.createUdf(name(name), new Type[] {arg}, IntegerType.BIGINT,
                false, TFunctionBinaryType.SRJAR, "hdfs://host/function.jar", "example.Function", null, null, null);
        function.setFunctionId(id);
        function.setChecksum("checksum");
        return function;
    }

    private static List<Function> variants() {
        AggregateFunction aggregate = AggregateFunction.AggregateFunctionBuilder.createUdfBuilder(TFunctionBinaryType.PYTHON)
                .name(name("aggregate"))
                .argsType(new Type[] {IntegerType.INT}).retType(IntegerType.BIGINT).hasVarArgs(false)
                .intermediateType(VarcharType.VARCHAR).objectFile("hdfs://host/function.py")
                .symbolName("aggregate").inputType("arrow").setIsolationType(false).build();
        aggregate.setFunctionId(-12);
        TableFunction table = new TableFunction(name("table_fn"), List.of("column"), List.of(IntegerType.INT),
                List.of(VarcharType.VARCHAR), false);
        table.setFunctionId(-13);
        SqlFunction sql = new SqlFunction(name("sql_fn"), new Type[] {IntegerType.INT}, IntegerType.BIGINT,
                new String[] {"x"}, "x + 1");
        sql.setFunctionId(-14);
        return List.of(scalar("scalar", IntegerType.INT, -11), aggregate, table, sql);
    }

    private GlobalFunctionMgr v2(GlobalFunctionMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        GlobalFunctionMgr result = new GlobalFunctionMgr();
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        result.load(reader);
        reader.close();
        return result;
    }

    private GlobalFunctionMgr v3(GlobalFunctionMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        GlobalFunctionMgr result = new GlobalFunctionMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, result.getImageV3Loaders());
        assertEquals(260, sections.get(0).getType());
        assertEquals(0, sections.get(0).getManagerLength());
        return result;
    }

    private static Map<String, Function> bySignature(GlobalFunctionMgr manager) {
        return manager.getFunctions().stream().collect(Collectors.toMap(Function::signatureString, value -> value));
    }

    @Test
    public void testAllFunctionSubtypesMatchV2() throws Exception {
        GlobalFunctionMgr source = new GlobalFunctionMgr();
        variants().forEach(source::replayAddFunction);
        GlobalFunctionMgr expected = v2(source);
        GlobalFunctionMgr restored = v3(source);
        GsonEquivalence.assertEquivalent(bySignature(expected), bySignature(restored));
        for (Function function : restored.getFunctions()) {
            Function original = bySignature(expected).get(function.signatureString());
            assertEquals(original.getClass(), function.getClass());
            assertEquals(original.getFunctionId(), function.getFunctionId());
            assertTrue(function.getFunctionName().isGlobalFunction());
        }

    }

    @Test
    public void testOverloadsAndReplacementAfterRestore() throws Exception {
        GlobalFunctionMgr source = new GlobalFunctionMgr();
        source.replayAddFunction(scalar("overload", IntegerType.INT, -1));
        source.replayAddFunction(scalar("overload", IntegerType.BIGINT, -2));
        GlobalFunctionMgr restored = v3(source);
        assertEquals(2, restored.getFunctions().size());
        ScalarFunction replacement = scalar("overload", IntegerType.INT, -3);
        restored.replayAddFunction(replacement);
        assertEquals(2, restored.getFunctions().size());
        assertEquals(Set.of(-2L, -3L), restored.getFunctions().stream()
                .map(Function::getFunctionId).collect(Collectors.toSet()));
        restored.replayDropFunction(new FunctionSearchDesc(name("overload"), new Type[] {IntegerType.INT}, false));
        assertEquals(1, restored.getFunctions().size());
        assertEquals(-2L, restored.getFunctions().get(0).getFunctionId());

    }

    @Test
    public void testUnknownSubtypeHandledByReader() throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, writer -> {
            try (SectionWriter section = writer.beginSection(SectionId.GLOBAL_FUNCTION_MGR)) {
                section.writeEntry(scalar("before", IntegerType.INT, -1).toProto());
                section.writeEntry(FunctionPB.newBuilder().setKindValue(999).build());
                section.writeEntry(scalar("after", IntegerType.INT, -2).toProto());
            }
        });
        boolean old = Config.metadata_ignore_unknown_subtype;
        try {
            Config.metadata_ignore_unknown_subtype = false;
            GlobalFunctionMgr strict = new GlobalFunctionMgr();
            assertThrows(ImageFormatException.class, () -> LoadImageTestSupport.load(path, strict.getImageV3Loaders()));
            Config.metadata_ignore_unknown_subtype = true;
            GlobalFunctionMgr restored = new GlobalFunctionMgr();
            LoadImageTestSupport.load(path, restored.getImageV3Loaders());
            assertEquals(Set.of("before", "after"), restored.getFunctions().stream()
                    .map(f -> f.getFunctionName().getFunction()).collect(Collectors.toSet()));
        } finally {
            Config.metadata_ignore_unknown_subtype = old;
        }

    }

    @Test
    public void testManyFunctionsAreIndividualEntries() throws Exception {
        GlobalFunctionMgr source = new GlobalFunctionMgr();
        for (int i = 0; i < 1024; i++) {
            source.replayAddFunction(scalar("function_" + i, IntegerType.INT, -i - 1));
        }
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        GlobalFunctionMgr restored = new GlobalFunctionMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(1024, sections.get(0).getNumEntries());
        assertEquals(1024, restored.getFunctions().size());
        assertEquals(bySignature(source).keySet(), bySignature(restored).keySet());

    }

    @Test
    public void testEmptyManager() throws Exception {
        assertTrue(v3(new GlobalFunctionMgr()).getFunctions().isEmpty());

    }
}
