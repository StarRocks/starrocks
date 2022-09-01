// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.qe;

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

public class QueryDetailTest {
    @Test
    public void testQueryDetail() {
        QueryDetail queryDetail = new QueryDetail("219a2d5443c542d4-8fc938db37c892e3", true, 1, "127.0.0.1",
                System.currentTimeMillis(), -1, -1, QueryDetail.QueryMemState.RUNNING,
                "testDb", "select * from table1 limit 1",
                "root", "");
        queryDetail.setProfile("bbbbb");
        queryDetail.setErrorMessage("cancelled");

        QueryDetail copyOfQueryDetail = queryDetail.copy();
        Gson gson = new Gson();
        Assert.assertEquals(gson.toJson(queryDetail), gson.toJson(copyOfQueryDetail));

        queryDetail.setLatency(10);
        Assert.assertEquals(-1, copyOfQueryDetail.getLatency());
    }
}
