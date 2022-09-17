// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.DataDescription;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class BrokerFileGroupTest {
    @Mocked
    private Auth auth;
    @Mocked
    private Database db;
    @Mocked
    private OlapTable olapTable;
    @Mocked
    private HiveTable hiveTable;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedAuth(auth);
    }

    @Test
    public void testParseHiveTable() throws UserException {
        // k1 = bitmap_dict(k1)
        SlotRef slotRef1 = new SlotRef(null, "k1");
        List<Expr> params1 = Lists.newArrayList(slotRef1);
        BinaryPredicate predicate1 = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef1,
                new FunctionCallExpr("bitmap_dict", params1));

        // k3 = k2 + 1
        SlotRef slotRef2 = new SlotRef(null, "k2");
        SlotRef slotRef3 = new SlotRef(null, "k3");
        BinaryPredicate predicate2 = new BinaryPredicate(
                BinaryPredicate.Operator.EQ, slotRef3,
                new ArithmeticExpr(ArithmeticExpr.Operator.ADD, slotRef2, new IntLiteral(1, Type.INT)));
        DataDescription desc = new DataDescription("olapTable", null, "hiveTable", false,
                Lists.newArrayList(predicate1, predicate2), null);
        desc.analyze("testDb");

        // schema
        Column k1 = new Column("k1", Type.BITMAP);
        Column k2 = new Column("k2", Type.INT);
        Column k3 = new Column("k3", Type.INT);
        Column k4 = new Column("k4", Type.INT);

        new Expectations() {
            {
                db.getTable("olapTable");
                result = olapTable;
                db.getTable("hiveTable");
                result = hiveTable;
                olapTable.getBaseSchema();
                result = Lists.newArrayList(k3, k1);
                hiveTable.getBaseSchema();
                result = Lists.newArrayList(k1, k2, k4);
                hiveTable.getId();
                result = 10;
            }
        };

        BrokerFileGroup fileGroup = new BrokerFileGroup(desc);
        fileGroup.parse(db, desc);
        Assert.assertEquals(Lists.newArrayList("k1", "k2"), fileGroup.getFileFieldNames());
        Assert.assertEquals(10, fileGroup.getSrcTableId());
    }
}
