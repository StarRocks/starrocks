package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.PartitionColPredicateExtractor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class PartitionColPredicateExtractorTest {

    private static RangePartitionInfo rangePartitionInfo;
    private static ColumnRefFactory factory = new ColumnRefFactory();
    private static Column partitionCol = new Column("date_col", Type.DATE, false);
    private static Column intCol = new Column("int_col", Type.INT, false);
    private static Column charCol = new Column("char_col", Type.CHAR, false);
    private static Column varcharCol = new Column("varchar_col", Type.VARCHAR, true);
    private static ColumnRefOperator partitionRef = factory.create("date_col", Type.DATE, false);
    private static ColumnRefOperator intRef = factory.create("int_col", Type.INT, false);
    private static ColumnRefOperator charRef = factory.create("char_col", Type.CHAR, false);
    private static ColumnRefOperator varcharRef = factory.create("varchar_col", Type.VARCHAR, true);
    private static Map<Column, ColumnRefOperator> columnMetaToColRefMap = Maps.newHashMap();

    @BeforeAll
    public static void before() {
        rangePartitionInfo = new RangePartitionInfo(ImmutableList.of(partitionCol));
        columnMetaToColRefMap.put(partitionCol, partitionRef);
        columnMetaToColRefMap.put(intCol, intRef);
        columnMetaToColRefMap.put(charCol, charRef);
        columnMetaToColRefMap.put(varcharCol, varcharRef);
    }

    @ParameterizedTest
    @MethodSource("provideScalarOperator")
    public void testPredicateExtractor(ScalarOperator predicate) {
        PartitionColPredicateExtractor extractor =
                new PartitionColPredicateExtractor(rangePartitionInfo, columnMetaToColRefMap);
        ScalarOperator newPredicate = predicate.clone().accept(extractor, null);
        System.out.println("predicate: " + predicate.debugString());
        System.out.println("newPredicate: " + newPredicate.debugString());
    }

    private static Stream<Arguments> provideScalarOperator() {
        CallOperator callOperator = new CallOperator(FunctionSet.YEAR, Type.DOUBLE, Lists.newArrayList(partitionRef));

        BinaryPredicateOperator binaryPred_1 =
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, intRef,
                        ConstantOperator.createInt(150));

        BinaryPredicateOperator binaryPred_2 =
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, callOperator,
                        ConstantOperator.createInt(150));

        InPredicateOperator inPredicateOperator =
                new InPredicateOperator(partitionRef, ConstantOperator.createDate(LocalDateTime.now()));

        CompoundPredicateOperator compoundPredicateOperator =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, inPredicateOperator,
                        binaryPred_1);

        LikePredicateOperator likePredicateOperator =
                new LikePredicateOperator(callOperator, ConstantOperator.createInt(150));

        List<ScalarOperator> operators =
                Lists.newArrayList(binaryPred_1, binaryPred_2, inPredicateOperator, compoundPredicateOperator,
                        likePredicateOperator);

        CompoundPredicateOperator.CompoundType[] compoundTypes = CompoundPredicateOperator.CompoundType.values();
        BinaryPredicateOperator.BinaryType[] binaryTypes = BinaryPredicateOperator.BinaryType.values();
        ScalarOperator left = binaryPred_1;
        for (int i = 0; i < 300; i++) {
            CompoundPredicateOperator.CompoundType compoundType = compoundTypes[i % compoundTypes.length];
            BinaryPredicateOperator.BinaryType binaryType = binaryTypes[i % binaryTypes.length];
            ScalarOperator first = new BinaryPredicateOperator(binaryType, intRef, ConstantOperator.createInt(150));
            ScalarOperator second =
                    new BinaryPredicateOperator(binaryType, partitionRef, ConstantOperator.createInt(150));
            ScalarOperator right;
            if (compoundType == CompoundPredicateOperator.CompoundType.NOT) {
                right = new CompoundPredicateOperator(compoundType, first);
            } else {
                right = new CompoundPredicateOperator(compoundType, first, second);
            }

            left = new CompoundPredicateOperator(compoundType, left, right);
            operators.add(left);
        }
        return operators.stream().map(op -> Arguments.arguments(op));
    }

}
