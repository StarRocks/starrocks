package com.starrocks.catalog;

import com.starrocks.analysis.IntLiteral;
import org.junit.Assert;
import org.junit.Test;

public class HivePartitionKeyTest {

    @Test
    public void testEquals() throws Exception {
        PartitionKey hivePartitionKey = new HivePartitionKey();
        hivePartitionKey.getKeys().add(new IntLiteral(5));
        hivePartitionKey.getKeys().add(new IntLiteral(6));
        hivePartitionKey.getTypes().add(PrimitiveType.INT);
        hivePartitionKey.getTypes().add(PrimitiveType.INT);

        PartitionKey anotherHivePartitionKey = new HivePartitionKey();
        anotherHivePartitionKey.getKeys().add(new IntLiteral(5));
        hivePartitionKey.getTypes().add(PrimitiveType.INT);

        Assert.assertNotEquals(hivePartitionKey, anotherHivePartitionKey);
        Assert.assertEquals(hivePartitionKey, hivePartitionKey);

        anotherHivePartitionKey = new HudiPartitionKey();
        Assert.assertNotEquals(hivePartitionKey, anotherHivePartitionKey);

        hivePartitionKey = new HivePartitionKey();
        hivePartitionKey.getKeys().add(new IntLiteral("6", Type.INT));
        hivePartitionKey.getTypes().add(PrimitiveType.INT);
        anotherHivePartitionKey = new HivePartitionKey();
        anotherHivePartitionKey.getKeys().add(new IntLiteral("06", Type.INT));
        anotherHivePartitionKey.getTypes().add(PrimitiveType.INT);
        Assert.assertNotEquals(hivePartitionKey, anotherHivePartitionKey);
    }
}
