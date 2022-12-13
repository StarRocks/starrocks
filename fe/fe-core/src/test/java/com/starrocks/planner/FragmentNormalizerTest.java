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


package com.starrocks.planner;

import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class FragmentNormalizerTest {

    private void testHelper(Column partitionColumn, LiteralExpr lower, LiteralExpr lowerSucc, LiteralExpr upper,
                            LiteralExpr upperSucc)
            throws AnalysisException {
        List<Column> partitionColumns = Arrays.asList(partitionColumn);
        PartitionKey minKey = PartitionKey.createInfinityPartitionKey(partitionColumns, false);
        PartitionKey maxKey = PartitionKey.createInfinityPartitionKey(partitionColumns, true);
        PartitionKey lowerKey = new PartitionKey();
        PartitionKey lowerSuccKey = new PartitionKey();
        PartitionKey upperKey = new PartitionKey();
        PartitionKey upperSuccKey = new PartitionKey();
        lowerKey.pushColumn(lower, partitionColumn.getPrimitiveType());
        lowerSuccKey.pushColumn(lowerSucc, partitionColumn.getPrimitiveType());
        upperKey.pushColumn(upper, partitionColumn.getPrimitiveType());
        upperSuccKey.pushColumn(upperSucc, partitionColumn.getPrimitiveType());

        Assert.assertEquals(lowerKey.successor(), lowerSuccKey);
        Assert.assertEquals(upperKey.successor(), upperSuccKey);
        Assert.assertEquals(maxKey.successor(), maxKey);

        Object[][] cases = new Object[][] {{Range.open(lowerKey, upperKey), Range.closedOpen(lowerSuccKey, upperKey)},
                {Range.openClosed(lowerKey, upperKey), Range.closedOpen(lowerSuccKey, upperSuccKey)},
                {Range.closedOpen(lowerKey, upperKey), Range.closedOpen(lowerKey, upperKey)},
                {Range.closed(lowerKey, upperKey), Range.closedOpen(lowerKey, upperSuccKey)},
                {Range.closedOpen(lowerKey, maxKey), Range.closedOpen(lowerKey, maxKey)},
                {Range.open(lowerKey, maxKey), Range.closedOpen(lowerSuccKey, maxKey)},
                {Range.closedOpen(minKey, upperKey), Range.closedOpen(minKey, upperKey)},
                {Range.closed(minKey, upperKey), Range.closedOpen(minKey, upperSuccKey)},
                {Range.closed(upperKey, upperKey), Range.closedOpen(upperKey, upperSuccKey)}};

        for (Object[] tc : cases) {
            Range<PartitionKey> range = (Range<PartitionKey>) tc[0];
            Range<PartitionKey> targetRange = (Range<PartitionKey>) tc[1];
            Assert.assertEquals(targetRange, FragmentNormalizer.toClosedOpenRange(range));
        }
    }

    @Test
    public void testToClosedAndOpenRangeForDate() throws AnalysisException {
        Column partitionColumn = new Column("dt", Type.fromPrimitiveType(PrimitiveType.DATE));
        LiteralExpr lower = new DateLiteral(2022, 1, 1);
        LiteralExpr lowerSucc = new DateLiteral(2022, 1, 2);
        LiteralExpr upper = new DateLiteral(2022, 1, 10);
        LiteralExpr upperSucc = new DateLiteral(2022, 1, 11);
        testHelper(partitionColumn, lower, lowerSucc, upper, upperSucc);
    }

    @Test
    public void testToClosedAndOpenRangeForDatetime() throws AnalysisException {
        Column partitionColumn = new Column("ts", Type.fromPrimitiveType(PrimitiveType.DATETIME));
        LiteralExpr lower = new DateLiteral(2022, 1, 1, 11, 23, 59, 0);
        LiteralExpr lowerSucc = new DateLiteral(2022, 1, 1, 11, 24, 0, 0);
        LiteralExpr upper = new DateLiteral(2022, 1, 10, 23, 59, 59, 0);
        LiteralExpr upperSucc = new DateLiteral(2022, 1, 11, 0, 0, 0, 0);
        testHelper(partitionColumn, lower, lowerSucc, upper, upperSucc);

    }

    @Test
    public void testToClosedAndOpenRangeForInteger() throws AnalysisException {
        PrimitiveType[] integerPtypes = new PrimitiveType[] {
                PrimitiveType.BIGINT,
                PrimitiveType.INT,
                PrimitiveType.SMALLINT,
                PrimitiveType.TINYINT,
        };
        for (PrimitiveType ptype : integerPtypes) {
            Type type = Type.fromPrimitiveType(ptype);
            long secondMaxValue = (1L << (ptype.getTypeSize() * 8 - 1)) - 2;
            Column partitionColumn = new Column("k0", type);
            LiteralExpr lower = new IntLiteral(1, type);
            LiteralExpr lowerSucc = new IntLiteral(2, type);
            LiteralExpr upper = new IntLiteral(secondMaxValue, type);
            LiteralExpr upperSucc = new IntLiteral(secondMaxValue + 1, type);
            testHelper(partitionColumn, lower, lowerSucc, upper, upperSucc);
        }
    }

    @Test
    public void testToClosedAndOpenRangeForLargeInt() throws AnalysisException {
        Column partitionColumn = new Column("k0", Type.fromPrimitiveType(PrimitiveType.LARGEINT));
        LiteralExpr lower = new LargeIntLiteral("1");
        LiteralExpr lowerSucc = new LargeIntLiteral("2");
        LiteralExpr upper =
                new LargeIntLiteral(BigInteger.ONE.shiftLeft(127).subtract(BigInteger.valueOf(2)).toString());
        LiteralExpr upperSucc =
                new LargeIntLiteral(BigInteger.ONE.shiftLeft(127).subtract(BigInteger.valueOf(1)).toString());
        testHelper(partitionColumn, lower, lowerSucc, upper, upperSucc);
    }
}
