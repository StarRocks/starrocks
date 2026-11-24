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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.google.protobuf.ProtocolMessageEnum;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.SqlFunction;
import com.starrocks.catalog.TableFunction;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.AggStateDescPB;
import com.starrocks.persist.proto.ColumnPB;
import com.starrocks.persist.proto.FunctionCorePB;
import com.starrocks.persist.proto.FunctionPB;
import com.starrocks.persist.proto.ScalarTypePB;
import com.starrocks.persist.proto.TypePB;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.HdfsURI;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.AggStateDesc;
import com.starrocks.type.AnyArrayType;
import com.starrocks.type.AnyElementType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.PseudoType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.TypeProtoConverter;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;

/**
 * The type.proto, column.proto and function.proto conversions deliver the same objects as a v2 (GSON) image round trip.
 */
public class CatalogTypeProtoTest {

    /**
     * A type is only ever persisted as a field of something else. That is also where GSON tolerates the unregistered
     * ScalarType subclasses (IntegerType, DecimalType, ...): as a field they are written reflectively without a class
     * label and read back as a plain ScalarType.
     */
    private static class TypeHolder {
        @SerializedName("t")
        Type type;
    }

    private static Type viaGson(Type type) {
        TypeHolder holder = new TypeHolder();
        holder.type = type;
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(holder), TypeHolder.class).type;
    }

    private static <T> T viaGson(T value, Class<T> clazz) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(value, clazz), clazz);
    }

    private static Type viaProto(Type type) throws Exception {
        return TypeProtoConverter.fromProto(TypePB.parseFrom(assertInstances(TypeProtoConverter.toProto(type)).toByteArray()));
    }

    private static Column viaProto(Column column) throws Exception {
        return Column.fromProto(ColumnPB.parseFrom(assertInstances(column.toProto()).toByteArray()));
    }

    private static Function viaProto(Function function) throws Exception {
        return Function.fromProto(FunctionPB.parseFrom(assertInstances(function.toProto()).toByteArray()));
    }

    private static StructType namedStruct() {
        return new StructType(Lists.newArrayList(
                new StructField("a", 1, "a_phys", IntegerType.INT, "first field"),
                new StructField("b", 2, new MapType(VarcharType.VARCHAR, DateType.DATE), null)));
    }

    @Test
    public void testTypesMatchGson() throws Exception {
        List<Type> types = Lists.newArrayList(
                IntegerType.INT,
                new ScalarType(PrimitiveType.FILE),
                new ScalarType(PrimitiveType.GEOGRAPHY),
                new ScalarType(PrimitiveType.GEOMETRY),
                BooleanType.BOOLEAN,
                DateType.DATETIME,
                VarcharType.VARCHAR,
                TypeFactory.createCharType(10),
                TypeFactory.createVarcharType(255),
                DecimalType.DECIMAL64,
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 9),
                new ArrayType(IntegerType.BIGINT),
                new MapType(VarcharType.VARCHAR, new ArrayType(DateType.DATE)),
                namedStruct(),
                new StructType(Lists.newArrayList(IntegerType.INT, VarcharType.VARCHAR)),
                new ArrayType(namedStruct()),
                new PseudoType(),
                AnyElementType.ANY_ELEMENT,
                AnyArrayType.ANY_ARRAY);
        for (Type type : types) {
            Type expected = viaGson(type);
            Type actual = viaProto(type);
            GsonEquivalence.assertEquivalent(expected, actual);
            Assertions.assertEquals(expected.getClass(), actual.getClass(), type::toString);
            if (!(expected instanceof PseudoType)) {
                // A plain PseudoType prints its identity hash, so its SQL form is never stable.
                Assertions.assertEquals(expected.toSql(), actual.toSql());
            }
        }
        // Whatever the runtime subclass was, both formats deliver a plain ScalarType.
        Assertions.assertSame(ScalarType.class, viaProto(IntegerType.INT).getClass());
        Assertions.assertSame(ScalarType.class, viaProto(DecimalType.DECIMAL64).getClass());
    }

    @Test
    public void testAggStateDescMatchesGson() throws Exception {
        AggStateDesc desc = new AggStateDesc("sum", IntegerType.BIGINT, Lists.newArrayList(IntegerType.INT), true);
        AggStateDesc expected = viaGson(desc, AggStateDesc.class);
        AggStateDesc actual = TypeProtoConverter.fromProto(
                AggStateDescPB.parseFrom(TypeProtoConverter.toProto(desc).toByteArray()));
        GsonEquivalence.assertEquivalent(expected, actual);
    }

    @Test
    public void testColumnsMatchGson() throws Exception {
        AggStateDesc aggStateDesc = new AggStateDesc("sum", IntegerType.BIGINT, Lists.newArrayList(IntegerType.INT), true);
        Column keyColumn = new Column("k1", IntegerType.INT, true, null, "0", "the key");
        Column sumColumn = new Column("v1", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4), false,
                AggregateType.SUM, true, null, "");
        Column aggStateColumn = new Column("v2", IntegerType.BIGINT, false, AggregateType.AGG_STATE_UNION, aggStateDesc,
                true, null, "agg state", 7, "v2_phys");
        Column timeDefaultColumn = new Column("dt", DateType.DATETIME, false, null, false,
                new ColumnDef.DefaultValueDef(true, true, SqlParser.parseSqlToExpr("now(3)", SqlModeHelper.MODE_DEFAULT)),
                "");
        Column generatedColumn = new Column("g1", IntegerType.BIGINT, false, null, true, null, "generated");
        generatedColumn.setGeneratedColumnExpr(ColumnIdExpr.fromSql("k1 + 1"));
        generatedColumn.setUniqueId(9);
        Column hiddenColumn = new Column("__row", IntegerType.BIGINT, false, null, false, null, null);
        hiddenColumn.setIsHidden(true);
        hiddenColumn.setIsAutoIncrement(true);

        for (Column column : List.of(keyColumn, sumColumn, aggStateColumn, timeDefaultColumn, generatedColumn,
                hiddenColumn)) {
            Column expected = viaGson(column, Column.class);
            Column actual = viaProto(column);
            GsonEquivalence.assertEquivalent(expected, actual);
        }

        // The derived state is rebuilt the way the GSON hook does it.
        Column actualGenerated = viaProto(generatedColumn);
        Assertions.assertTrue(actualGenerated.isGeneratedColumn());
        Column actualAggState = viaProto(aggStateColumn);
        Assertions.assertSame(actualAggState.getAggStateDesc(), actualAggState.getType().getAggStateDesc());
        Column actualTimeDefault = viaProto(timeDefaultColumn);
        Assertions.assertEquals("now(3)", actualTimeDefault.getDefaultExpr().getExpr());
        Assertions.assertTrue(actualTimeDefault.getDefaultExpr().hasArgs());
    }

    @Test
    public void testComplexDefaultExprMatchesGson() throws Exception {
        Column column = new Column("arr", new ArrayType(IntegerType.INT), false, null, true,
                new ColumnDef.DefaultValueDef(true, SqlParser.parseSqlToExpr("[1, 2, 3]", SqlModeHelper.MODE_DEFAULT)),
                "");
        Column expected = viaGson(column, Column.class);
        Column actual = viaProto(column);
        GsonEquivalence.assertEquivalent(expected, actual);
        Assertions.assertNotNull(actual.getDefaultExpr().getExprObject());
    }

    @Test
    public void testFunctionsMatchGson() throws Exception {
        ScalarFunction scalarUdf = ScalarFunction.createUdf(new FunctionName("db1", "my_udf"),
                new Type[] {IntegerType.INT, VarcharType.VARCHAR}, IntegerType.BIGINT, false, TFunctionBinaryType.SRJAR,
                "hdfs://host/udf.jar", "com.example.MyUdf", null, null, null);
        scalarUdf.setFunctionId(1001);
        scalarUdf.setChecksum("abc");
        scalarUdf.setArgNames(Lists.newArrayList("a", "b"));

        AggregateFunction aggregateUdf = AggregateFunction.AggregateFunctionBuilder
                .createUdfBuilder(TFunctionBinaryType.PYTHON)
                .name(new FunctionName("db1", "my_agg"))
                .argsType(new Type[] {IntegerType.INT})
                .retType(IntegerType.BIGINT)
                .hasVarArgs(false)
                .intermediateType(VarcharType.VARCHAR)
                .objectFile("hdfs://host/agg.py")
                .symbolName("agg")
                .inputType("arrow")
                .setIsolationType(false)
                .build();
        aggregateUdf.setFunctionId(1002);

        TableFunction tableUdf = new TableFunction(new FunctionName("db1", "my_udtf"), Lists.newArrayList("my_udtf"),
                Lists.newArrayList(IntegerType.INT), Lists.newArrayList(VarcharType.VARCHAR), false);
        tableUdf.setBinaryType(TFunctionBinaryType.SRJAR);
        tableUdf.setChecksum("def");
        tableUdf.setLocation(new HdfsURI("hdfs://host/udtf.jar"));
        tableUdf.setSymbolName("com.example.MyUdtf");
        tableUdf.setCloudConfiguration(new CloudConfiguration());
        tableUdf.setFunctionId(1003);

        SqlFunction sqlFunction = new SqlFunction(new FunctionName("db1", "my_sql_fn"), new Type[] {IntegerType.INT},
                IntegerType.BIGINT, new String[] {"x"}, "x + 1");
        sqlFunction.setFunctionId(1004);

        for (Function function : List.of(scalarUdf, aggregateUdf, tableUdf, sqlFunction)) {
            Function expected = viaGson(function, Function.class);
            Function actual = viaProto(function);
            GsonEquivalence.assertEquivalent(expected, actual);
            Assertions.assertEquals(expected.getClass(), actual.getClass());
        }
    }

    @Test
    public void testEnumMappingsCoverEveryConstant() {
        assertBijection(PrimitiveType.class, ScalarTypePB.PrimitiveType.class);
        assertBijection(AggregateType.class, ColumnPB.AggregateType.class);
        assertBijection(TFunctionBinaryType.class, FunctionCorePB.BinaryType.class);

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ProtoEnums.fromProto(ScalarTypePB.PrimitiveType.UNRECOGNIZED, PrimitiveType.class));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ProtoEnums.fromProto(ScalarTypePB.PrimitiveType.UNSPECIFIED, PrimitiveType.class));
    }

    private static <J extends Enum<J>, P extends Enum<P> & ProtocolMessageEnum> void assertBijection(
            Class<J> javaEnum, Class<P> protoEnum) {
        for (J value : javaEnum.getEnumConstants()) {
            Assertions.assertSame(value, ProtoEnums.fromProto(ProtoEnums.toProto(value, protoEnum), javaEnum));
        }
        for (P value : protoEnum.getEnumConstants()) {
            if ("UNRECOGNIZED".equals(value.name()) || value.name().endsWith("UNSPECIFIED")) {
                continue;
            }
            Assertions.assertSame(value, ProtoEnums.toProto(ProtoEnums.fromProto(value, javaEnum), protoEnum));
        }
    }
}
