// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule;

import com.google.common.collect.Lists;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.ListPartitionForOlapPruner;
import com.starrocks.planner.PartitionColumnFilter;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListPartitionForOlapPrunerTest {

    private Map<Long, List<LiteralExpr>> literalExprValues;
    private Map<Long, List<List<LiteralExpr>>> multiLiteralExprValues;
    private List<Column> partitionColumns;
    private Map<String, PartitionColumnFilter> columnFilters;
    private ListPartitionForOlapPruner pruner;

    private void initSingleItemPartitionPruner() throws AnalysisException {
        partitionColumns = new ArrayList<>();
        partitionColumns.add(new Column("province", Type.STRING));

        columnFilters = new HashMap<>();
        PartitionColumnFilter partitionColumnFilter = new PartitionColumnFilter();
        columnFilters.put("province", partitionColumnFilter);

        List<LiteralExpr> p1 = Lists.newArrayList(
                new PartitionValue("guangdong").getValue(Type.STRING),
                new PartitionValue("shanghai").getValue(Type.STRING));

        List<LiteralExpr> p2 = Lists.newArrayList(
                new PartitionValue("beijing").getValue(Type.STRING),
                new PartitionValue("chongqing").getValue(Type.STRING));

        literalExprValues = new HashMap<>();
        literalExprValues.put(10001L,p1);
        literalExprValues.put(10002L,p2);
        pruner = new ListPartitionForOlapPruner(literalExprValues, multiLiteralExprValues,
                partitionColumns, columnFilters);

    }

    private void initMultiItemPartitionPruner() throws AnalysisException {
        partitionColumns = new ArrayList<>();
        partitionColumns.add(new Column("dt", Type.DATE));
        partitionColumns.add(new Column("province", Type.STRING));

        columnFilters = new HashMap<>();
        PartitionColumnFilter provinceFilter = new PartitionColumnFilter();
        columnFilters.put("province", provinceFilter);
        PartitionColumnFilter dtFilter = new PartitionColumnFilter();
        columnFilters.put("dt", dtFilter);

        List<List<LiteralExpr>> p1 = new ArrayList<>();
        List<LiteralExpr> pItem1 = Lists.newArrayList(
                new PartitionValue("2022-04-01").getValue(Type.DATE),
                new PartitionValue("beijing").getValue(Type.STRING));
        p1.add(pItem1);

        List<List<LiteralExpr>> p2 = new ArrayList<>();
        List<LiteralExpr> pItem2 = Lists.newArrayList(
                new PartitionValue("2022-04-02").getValue(Type.DATE),
                new PartitionValue("shanghai").getValue(Type.STRING));
        p2.add(pItem2);

        multiLiteralExprValues = new HashMap<>();
        multiLiteralExprValues.put(10001L,p1);
        multiLiteralExprValues.put(10002L,p2);
        pruner = new ListPartitionForOlapPruner(literalExprValues, multiLiteralExprValues,
                partitionColumns, columnFilters);

    }

    @Test
    public void testSingleItemReturnNullPartition() throws AnalysisException {
        this.initSingleItemPartitionPruner();
        PartitionColumnFilter partitionColumnFilter = columnFilters.get("province");
        // 1. partitionColumnFilter is empty .
        // eg: select * from t_province_detail
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setLowerBound(new PartitionValue("guangdong").getValue(Type.STRING), true);
        partitionColumnFilter.setUpperBound(new PartitionValue("shanghai").getValue(Type.STRING), false);
        // 2. partitionColumnFilter is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where province >= 'guangdong' and province < 'shanghai'
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setLowerBound(new PartitionValue("guangdong").getValue(Type.STRING), true);
        // 3. partitionColumnFilter is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where province >= 'guangdong'
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setLowerBound(new PartitionValue("guangdong").getValue(Type.STRING), false);
        // 4. partitionColumnFilter is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where province > 'guangdong'
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setUpperBound(new PartitionValue("shanghai").getValue(Type.STRING), true);
        // 5. partitionColumnFilter is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where province <= 'shanghai'
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setUpperBound(new PartitionValue("shanghai").getValue(Type.STRING), false);
        // 6. partitionColumnFilter is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where province < 'shanghai'
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setLowerBound(new PartitionValue("guangdong").getValue(Type.STRING), true);
        partitionColumnFilter.setUpperBound(new PartitionValue("shanghai").getValue(Type.STRING), true);
        // 7. partitionColumnFilter is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where province between 'guangdong' and 'shanghai'
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setLowerBound(new PartitionValue("guangdong").getValue(Type.STRING), false);
        partitionColumnFilter.setUpperBound(new PartitionValue("shanghai").getValue(Type.STRING), true);
        // 8. partitionColumnFilter is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where province > 'guangdong' and province <= 'shanghai'
        Assert.assertEquals(null, pruner.prune());
    }

    @Test
    public void testSingleItemReturnEmptyPartition() throws AnalysisException {
        this.initSingleItemPartitionPruner();
        PartitionColumnFilter partitionColumnFilter = columnFilters.get("province");
        // eg: select * from t_province_detail where province = 'tianjin'
        partitionColumnFilter.setLowerBound(new PartitionValue("tianjin").getValue(Type.STRING), true);
        partitionColumnFilter.setUpperBound(new PartitionValue("tianjin").getValue(Type.STRING), true);
        Assert.assertEquals(0, pruner.prune().size());

        // eg: select * from t_province_detail where province in ('tianjin', 'jilin')
        LiteralExpr literalExpr1 = new PartitionValue("tianjin").getValue(Type.STRING);
        LiteralExpr literalExpr2 = new PartitionValue("jilin").getValue(Type.STRING);
        partitionColumnFilter.setInPredicateLiterals(Lists.newArrayList(literalExpr1, literalExpr2));
        Assert.assertEquals(0, pruner.prune().size());
    }

    @Test
    public void testSingleItemReturnPartitionForEqualPre() throws AnalysisException {
        this.initSingleItemPartitionPruner();
        PartitionColumnFilter partitionColumnFilter = columnFilters.get("province");
        // eg: select * from t_province_detail where province = 'beijing'
        partitionColumnFilter.setLowerBound(new PartitionValue("beijing").getValue(Type.STRING), true);
        partitionColumnFilter.setUpperBound(new PartitionValue("beijing").getValue(Type.STRING), true);
        long actual = pruner.prune().get(0);
        Assert.assertEquals(10002L, actual);
    }

    @Test
    public void testSingleItemReturnPartitionForInPre() throws AnalysisException {
        this.initSingleItemPartitionPruner();
        PartitionColumnFilter partitionColumnFilter = columnFilters.get("province");
        // eg: select * from t_province_detail where province in ('beijing', 'guangdong')
        LiteralExpr literalExpr1 = new PartitionValue("beijing").getValue(Type.STRING);
        LiteralExpr literalExpr2 = new PartitionValue("guangdong").getValue(Type.STRING);
        partitionColumnFilter.setInPredicateLiterals(Lists.newArrayList(literalExpr1, literalExpr2));
        List<Long> selectedPartitions = pruner.prune();
        Assert.assertEquals(2, selectedPartitions.size());
        long actual1 = selectedPartitions.get(0);
        long actual2 = selectedPartitions.get(1);
        Assert.assertEquals(10001L, actual1);
        Assert.assertEquals(10002L, actual2);
    }

    @Test
    public void testMultiItemReturnNullPartition() throws AnalysisException {
        this.initMultiItemPartitionPruner();
        PartitionColumnFilter partitionColumnFilter = columnFilters.get("province");
        // 1. partitionColumnFilter is empty .
        // eg: select * from t_province_detail
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setLowerBound(new PartitionValue("guangdong").getValue(Type.STRING), true);
        partitionColumnFilter.setUpperBound(new PartitionValue("shanghai").getValue(Type.STRING), false);
        // 2. partitionColumnFilter is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where province >= 'guangdong' and province < 'shanghai'
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setLowerBound(new PartitionValue("guangdong").getValue(Type.STRING), true);
        // 3. partitionColumnFilter is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where province >= 'guangdong'
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setLowerBound(new PartitionValue("guangdong").getValue(Type.STRING), false);
        // 4. partitionColumnFilter is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where province > 'guangdong'
        Assert.assertEquals(null, pruner.prune());

        PartitionColumnFilter filterDt = columnFilters.get("dt");
        partitionColumnFilter.setUpperBound(new PartitionValue("2022-04-01").getValue(Type.STRING), true);
        // 5. filterDt is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where dt <= '2022-04-01'
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setUpperBound(new PartitionValue("2022-04-01").getValue(Type.STRING), false);
        // 6. filterDt is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where dt < '2022-04-01'
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setLowerBound(new PartitionValue("2022-04-01").getValue(Type.STRING), true);
        partitionColumnFilter.setUpperBound(new PartitionValue("2022-04-02").getValue(Type.STRING), true);
        // 7. filterDt is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where dt between '2022-04-01' and '2022-04-02'
        Assert.assertEquals(null, pruner.prune());

        partitionColumnFilter.setLowerBound(new PartitionValue("2022-04-01").getValue(Type.STRING), false);
        partitionColumnFilter.setUpperBound(new PartitionValue("2022-04-02").getValue(Type.STRING), true);
        // 8. filterDt is not empty and lower not equal to upper value.
        // eg: select * from t_province_detail where dt > '2022-04-01' and province <= '2022-04-02'
        Assert.assertEquals(null, pruner.prune());
    }

    @Test
    public void testMultiItemReturnEmptyPartition() throws AnalysisException {
        this.initMultiItemPartitionPruner();

        // eg: select * from t_province_detail where province = 'shanxi'
        PartitionColumnFilter provinceFilter = columnFilters.get("province");
        provinceFilter.setLowerBound(new PartitionValue("shanxi").getValue(Type.STRING), true);
        provinceFilter.setUpperBound(new PartitionValue("shanxi").getValue(Type.STRING), true);
        List<Long> selectedPartitions = pruner.prune();
        Assert.assertEquals(0, selectedPartitions.size());

        // eg: select * from t_province_detail where province = 'shanxi' and dt in ('2022-05-02', '2022-05-01')
        PartitionColumnFilter dtFilter = columnFilters.get("dt");
        LiteralExpr literalExpr3 = new PartitionValue("2022-05-02").getValue(Type.DATE);
        LiteralExpr literalExpr4 = new PartitionValue("2022-05-01").getValue(Type.DATE);
        dtFilter.setInPredicateLiterals(Lists.newArrayList(literalExpr3, literalExpr4));

        selectedPartitions = pruner.prune();
        Assert.assertEquals(0, selectedPartitions.size());
    }

    @Test
    public void testMultiItemReturnPartitionForEqualPre() throws AnalysisException {
        this.initMultiItemPartitionPruner();

        // eg: select * from t_province_detail where province = 'beijing'
        PartitionColumnFilter provinceFilter = columnFilters.get("province");
        provinceFilter.setLowerBound(new PartitionValue("beijing").getValue(Type.STRING), true);
        provinceFilter.setUpperBound(new PartitionValue("beijing").getValue(Type.STRING), true);
        List<Long> selectedPartitions = pruner.prune();
        Assert.assertEquals(1, selectedPartitions.size());
        long actual1 = selectedPartitions.get(0);
        Assert.assertEquals(10001L, actual1);

        // eg: select * from t_province_detail where dt = '2022-04-02' and province = 'beijing'
        PartitionColumnFilter dtFilter = columnFilters.get("dt");
        dtFilter.setLowerBound(new PartitionValue("2022-04-02").getValue(Type.DATE), true);
        dtFilter.setUpperBound(new PartitionValue("2022-04-02").getValue(Type.DATE), true);
        selectedPartitions = pruner.prune();
        Assert.assertEquals(2, selectedPartitions.size());
        actual1 = selectedPartitions.get(0);
        long actual2 = selectedPartitions.get(1);
        Assert.assertEquals(10001L, actual1);
        Assert.assertEquals(10002L, actual2);
    }

    @Test
    public void testMultiItemReturnPartitionForInPre() throws AnalysisException {
        this.initMultiItemPartitionPruner();

        PartitionColumnFilter partitionColumnFilter = columnFilters.get("province");
        // eg: select * from t_province_detail where province in ('beijing', 'shanxi')
        LiteralExpr literalExpr1 = new PartitionValue("beijing").getValue(Type.STRING);
        LiteralExpr literalExpr2 = new PartitionValue("shanxi").getValue(Type.STRING);
        partitionColumnFilter.setInPredicateLiterals(Lists.newArrayList(literalExpr1, literalExpr2));

        List<Long> selectedPartitions = pruner.prune();
        Assert.assertEquals(1, selectedPartitions.size());
        long actual1 = selectedPartitions.get(0);
        Assert.assertEquals(10001L, actual1);

        // eg: select * from t_province_detail where province in ('beijing', 'shanxi') and dt in ('2022-04-02', '2022-05-01')
        PartitionColumnFilter dtFilter = columnFilters.get("dt");
        LiteralExpr literalExpr3 = new PartitionValue("2022-04-02").getValue(Type.DATE);
        LiteralExpr literalExpr4 = new PartitionValue("2022-05-01").getValue(Type.DATE);
        dtFilter.setInPredicateLiterals(Lists.newArrayList(literalExpr3, literalExpr4));

        selectedPartitions = pruner.prune();
        Assert.assertEquals(2, selectedPartitions.size());
        actual1 = selectedPartitions.get(0);
        long actual2 = selectedPartitions.get(1);
        Assert.assertEquals(10001L, actual1);
        Assert.assertEquals(10002L, actual2);
    }



}
