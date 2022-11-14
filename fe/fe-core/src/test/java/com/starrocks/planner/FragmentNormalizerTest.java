// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.planner;

import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class FragmentNormalizerTest {
    @Test
    public void testToClosedAndOpenRange() throws AnalysisException {
        Column partitionColumn = new Column("dt", Type.fromPrimitiveType(PrimitiveType.DATE));
        List<Column> partitionColumns = Arrays.asList(partitionColumn);
        PartitionKey minKey = PartitionKey.createInfinityPartitionKey(partitionColumns, false);
        PartitionKey maxKey = PartitionKey.createInfinityPartitionKey(partitionColumns, true);

        LiteralExpr lower = new DateLiteral(2022, 1, 1);
        LiteralExpr lowerSucc = new DateLiteral(2022, 1, 2);
        LiteralExpr upper = new DateLiteral(2022, 1, 10);
        LiteralExpr upperSucc = new DateLiteral(2022, 1, 11);

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
}
