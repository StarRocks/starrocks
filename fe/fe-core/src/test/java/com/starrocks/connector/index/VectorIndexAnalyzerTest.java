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

package com.starrocks.connector.index;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Coverage;
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Operation;
import com.starrocks.connector.index.ConnectorIndexMetadata.TableType;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class VectorIndexAnalyzerTest {
    private final ColumnRefOperator vectorColumn =
            new ColumnRefOperator(3, ArrayType.ARRAY_FLOAT, "embedding", true);
    private final ConnectorIndexMetadata metadata = ConnectorIndexMetadata.of(42, TableType.DATA_EVOLUTION,
            List.of(vectorDescriptor(VectorIndexMetric.L2), vectorDescriptor(VectorIndexMetric.COSINE),
                    vectorDescriptor(VectorIndexMetric.INNER_PRODUCT)));

    @Test
    public void testVectorTopNRecognitionRequiresCompatibleDirection() {
        ArrayOperator queryVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));
        CallOperator l2 = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(vectorColumn, queryVector));
        CallOperator cosine = new CallOperator(FunctionSet.APPROX_COSINE_SIMILARITY, FloatType.FLOAT,
                List.of(queryVector, vectorColumn));

        IndexAnalyzer analyzer = new IndexAnalyzer(metadata);
        Assertions.assertFalse(analyzer.supportsVectorTopN(ConstantOperator.createFloat(1), true));
        Assertions.assertTrue(analyzer.supportsVectorTopN(l2, true));
        Assertions.assertFalse(analyzer.supportsVectorTopN(l2, false));
        Assertions.assertTrue(analyzer.supportsVectorTopN(cosine, false));
        Assertions.assertFalse(analyzer.supportsVectorTopN(cosine, true));

        CallOperator innerProduct = new CallOperator(FunctionSet.APPROX_INNER_PRODUCT, FloatType.FLOAT,
                List.of(vectorColumn, new CastOperator(ArrayType.ARRAY_FLOAT, queryVector)));
        Assertions.assertTrue(analyzer.supportsVectorTopN(innerProduct, false));
        Assertions.assertTrue(analyzer.supportsVectorTopN(new CastOperator(FloatType.DOUBLE, l2), true));

        CallOperator unsupported = new CallOperator("unsupported_distance", FloatType.FLOAT,
                List.of(vectorColumn, queryVector));
        Assertions.assertFalse(analyzer.supportsVectorTopN(unsupported, true));

        ColumnRefOperator unindexed = new ColumnRefOperator(4, ArrayType.ARRAY_FLOAT, "other", true);
        CallOperator unindexedL2 = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(unindexed, queryVector));
        Assertions.assertFalse(analyzer.supportsVectorTopN(unindexedL2, true));

        ConnectorIndexMetadata cosineOnly = ConnectorIndexMetadata.of(
                42, TableType.DATA_EVOLUTION, List.of(vectorDescriptor(VectorIndexMetric.COSINE)));
        Assertions.assertFalse(new IndexAnalyzer(cosineOnly).supportsVectorTopN(l2, true));

        IndexAnalyzer.VectorTopNShape shape = IndexAnalyzer.analyzeVectorTopNShape(l2, true).orElseThrow();
        float[] vector = shape.getQueryVector();
        vector[0] = 99;
        Assertions.assertArrayEquals(new float[] {1, 2}, shape.getQueryVector());

        ArrayOperator emptyVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false, List.of());
        Assertions.assertTrue(IndexAnalyzer.analyzeVectorTopNShape(new CallOperator(
                FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT, List.of(vectorColumn, emptyVector)), true).isEmpty());
        ArrayOperator nullVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createNull(FloatType.FLOAT)));
        Assertions.assertTrue(IndexAnalyzer.analyzeVectorTopNShape(new CallOperator(
                FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT, List.of(vectorColumn, nullVector)), true).isEmpty());
        ArrayOperator nanVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createObject(Double.NaN, FloatType.FLOAT)));
        Assertions.assertTrue(IndexAnalyzer.analyzeVectorTopNShape(new CallOperator(
                FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT, List.of(vectorColumn, nanVector)), true).isEmpty());
    }

    @Test
    public void testVectorTopNRecognizesBoundStringCasts() {
        IndexAnalyzer analyzer = new IndexAnalyzer(metadata);
        for (ArrayType targetType : List.of(new ArrayType(FloatType.FLOAT), new ArrayType(FloatType.DOUBLE))) {
            CastOperator queryVector = new CastOperator(targetType,
                    ConstantOperator.createVarchar(" [ 1e0, +2.0 ] "));
            CallOperator score = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                    List.of(vectorColumn, queryVector));
            IndexAnalyzer.VectorTopNShape shape = IndexAnalyzer.analyzeVectorTopNShape(score, true).orElseThrow();
            Assertions.assertArrayEquals(new float[] {1, 2}, shape.getQueryVector());
            Assertions.assertTrue(analyzer.supportsVectorTopN(score, true));
        }

        CastOperator wrongDimension = new CastOperator(ArrayType.ARRAY_FLOAT,
                ConstantOperator.createVarchar("[1, 2, 3]"));
        Assertions.assertFalse(analyzer.supportsVectorTopN(new CallOperator(
                FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT, List.of(vectorColumn, wrongDimension)), true));
    }

    @Test
    public void testVectorTopNRejectsInvalidBoundStringCasts() {
        for (String literal : List.of("", "[]", "[ ]", "1,2", "[1,2", "1,2]", "[1,,2]", "[1,2,]",
                "[null,2]", "[NaN,2]", "[Infinity,2]", "[1e50,2]", "[[1],2]", "[invalid,2]",
                "[1f,2f]", "[1d,2d]", "[0x1p0,0x1p1]")) {
            CastOperator queryVector = new CastOperator(ArrayType.ARRAY_FLOAT, ConstantOperator.createVarchar(literal));
            Assertions.assertTrue(IndexAnalyzer.analyzeVectorTopNShape(new CallOperator(
                    FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT, List.of(vectorColumn, queryVector)), true).isEmpty(),
                    literal);
        }
        for (ScalarOperator queryVector : List.of(
                new CastOperator(ArrayType.ARRAY_FLOAT, ConstantOperator.createNull(VarcharType.VARCHAR)),
                ConstantOperator.createVarchar("[1,2]"),
                new CastOperator(new ArrayType(IntegerType.INT), ConstantOperator.createVarchar("[1,2]")))) {
            Assertions.assertTrue(IndexAnalyzer.analyzeVectorTopNShape(new CallOperator(
                    FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT, List.of(vectorColumn, queryVector)), true).isEmpty());
        }
    }

    @Test
    public void testVectorTopNRejectsValueChangingCasts() {
        ScalarOperator boundVector = new CastOperator(ArrayType.ARRAY_FLOAT,
                ConstantOperator.createVarchar("[1.9,2.9]"));
        ScalarOperator truncatedVector = new CastOperator(ArrayType.ARRAY_FLOAT,
                new CastOperator(new ArrayType(IntegerType.INT), boundVector));
        ScalarOperator truncatedElement = new ArrayOperator(ArrayType.ARRAY_FLOAT, false, List.of(
                new CastOperator(FloatType.FLOAT,
                        new CastOperator(IntegerType.INT, ConstantOperator.createFloat(1.9))),
                ConstantOperator.createFloat(2.9)));
        ScalarOperator narrowedElement = new ArrayOperator(ArrayType.ARRAY_DOUBLE, false, List.of(
                new CastOperator(FloatType.DOUBLE,
                        new CastOperator(FloatType.FLOAT, ConstantOperator.createDouble(1.9))),
                ConstantOperator.createDouble(2.9)));
        for (ScalarOperator queryVector : List.of(truncatedVector, truncatedElement, narrowedElement)) {
            CallOperator score = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                    List.of(vectorColumn, queryVector));
            Assertions.assertTrue(IndexAnalyzer.analyzeVectorTopNShape(score, true).isEmpty());
        }
    }

    @Test
    public void testVectorTopNPreservesSafeFloatingCastsAndDecimalLiterals() {
        ScalarOperator array = new ArrayOperator(ArrayType.ARRAY_FLOAT, false, List.of(
                new CastOperator(FloatType.FLOAT, ConstantOperator.createFloat(1.5)),
                ConstantOperator.createFloat(2)));
        ScalarOperator widenedElements = new ArrayOperator(ArrayType.ARRAY_DOUBLE, false, List.of(
                new CastOperator(FloatType.DOUBLE, ConstantOperator.createFloat(1.5)),
                ConstantOperator.createDouble(2)));
        ScalarOperator boundVector = new CastOperator(ArrayType.ARRAY_FLOAT,
                ConstantOperator.createVarchar("[ +.15e1, 2. ]"));
        for (ScalarOperator queryVector : List.of(array, widenedElements, boundVector,
                new CastOperator(ArrayType.ARRAY_DOUBLE, new CastOperator(ArrayType.ARRAY_FLOAT, array)))) {
            CallOperator score = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                    List.of(vectorColumn, queryVector));
            Assertions.assertArrayEquals(new float[] {1.5f, 2},
                    IndexAnalyzer.analyzeVectorTopNShape(score, true).orElseThrow().getQueryVector());
        }
    }

    @Test
    public void testVectorTopNRequiresMatchingDimensionAndNullSemantics() {
        ArrayOperator threeDimensions = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2),
                        ConstantOperator.createFloat(3)));
        CallOperator wrongDimension = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(vectorColumn, threeDimensions));
        Assertions.assertFalse(new IndexAnalyzer(metadata).supportsVectorTopN(wrongDimension, true));

        ConnectorIndexDescriptor nullableDescriptor = new ConnectorIndexDescriptor(
                IndexType.VECTOR, "test", vectorColumn.getId(), vectorColumn.getName(), true, false,
                Map.of(ConnectorIndexDescriptor.OPTION_METRIC, VectorIndexMetric.L2.name(),
                        ConnectorIndexDescriptor.OPTION_DIMENSION, "2"),
                Set.of(Operation.VECTOR_TOP_N), Coverage.FULL);
        IndexAnalyzer nullableAnalyzer = new IndexAnalyzer(ConnectorIndexMetadata.of(
                42, TableType.DATA_EVOLUTION, List.of(nullableDescriptor)));
        ArrayOperator queryVector = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));
        CallOperator score = new CallOperator(FunctionSet.APPROX_L2_DISTANCE, FloatType.FLOAT,
                List.of(vectorColumn, queryVector));
        IndexAnalyzer.VectorTopNShape shape = IndexAnalyzer.analyzeVectorTopNShape(score, true).orElseThrow();

        // ANN candidates omit NULL vectors. Until execution can replenish those rows, neither
        // ANN cannot represent leading nulls. NULLS LAST is still a valid request, but the
        // execution layer must replenish trailing null rows or fall back when candidates run short.
        Assertions.assertTrue(nullableAnalyzer.findVectorIndex(shape, true).isEmpty());
        Assertions.assertTrue(nullableAnalyzer.findVectorIndex(shape, false).isPresent());
        Assertions.assertTrue(new IndexAnalyzer(metadata).findVectorIndex(shape, true).isPresent());
    }

    private ConnectorIndexDescriptor vectorDescriptor(VectorIndexMetric metric) {
        return new ConnectorIndexDescriptor(IndexType.VECTOR, "test", vectorColumn.getId(),
                vectorColumn.getName(), false, false, Map.of(
                ConnectorIndexDescriptor.OPTION_METRIC, metric.name(),
                ConnectorIndexDescriptor.OPTION_DIMENSION, "2"),
                Set.of(Operation.VECTOR_TOP_N), Coverage.FULL);
    }
}
