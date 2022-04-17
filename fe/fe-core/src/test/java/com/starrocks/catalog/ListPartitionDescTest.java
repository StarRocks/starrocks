package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.analysis.*;
import com.starrocks.common.AnalysisException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListPartitionDescTest {

    private List<ColumnDef> findColumnDefList(){
        ColumnDef id = new ColumnDef("id",TypeDef.create(PrimitiveType.BIGINT));
        id.setAggregateType(AggregateType.NONE);
        ColumnDef userId = new ColumnDef("user_id",TypeDef.create(PrimitiveType.BIGINT));
        userId.setAggregateType(AggregateType.NONE);
        ColumnDef rechargeMoney = new ColumnDef("recharge_money",TypeDef.createDecimal(32,2));
        rechargeMoney.setAggregateType(AggregateType.NONE);
        ColumnDef province = new ColumnDef("province",TypeDef.createVarchar(64));
        province.setAggregateType(AggregateType.NONE);
        ColumnDef dt = new ColumnDef("dt",TypeDef.createVarchar(10));
        dt.setAggregateType(AggregateType.NONE);
        return  Lists.newArrayList(id,userId,rechargeMoney,province,dt);
    }

    private ListPartitionDesc findListMultiPartitionDesc(String colNames, String pName1,String pName2, Map<String,String> partitionProperties){
        List<String> partitionColNames = Lists.newArrayList(colNames.split(","));
        MultiListPartitionDesc p1 =
                new MultiListPartitionDesc( false, pName1,
                        Lists.newArrayList(Lists.newArrayList("2022-04-15","guangdong")
                                ,Lists.newArrayList("2022-04-15","tianjin")),partitionProperties);
        MultiListPartitionDesc p2 =
                new MultiListPartitionDesc( false, pName2,
                        Lists.newArrayList(Lists.newArrayList("2022-04-16","shanghai")
                                ,Lists.newArrayList("2022-04-16","beijing")),partitionProperties);
        List<PartitionDesc> partitionDescs = Lists.newArrayList(p1,p2);
        return new ListPartitionDesc(partitionColNames,partitionDescs);
    }

    private ListPartitionDesc findListSinglePartitionDesc(String colNames, String pName1,String pName2, Map<String,String> partitionProperties){
        List<String> partitionColNames = Lists.newArrayList(colNames.split(","));
        SingleListPartitionDesc p1 =
                new SingleListPartitionDesc( false, pName1,
                        Lists.newArrayList("guangdong","tianjin"),partitionProperties);
        SingleListPartitionDesc p2 =
                new SingleListPartitionDesc( false, pName2,
                        Lists.newArrayList("shanghai","beijing"),partitionProperties);
        List<PartitionDesc> partitionDescs = Lists.newArrayList(p1,p2);
        return new ListPartitionDesc(partitionColNames,partitionDescs);
    }

    private Map<String,String> findSupportedProperties(Map<String,String> properties){
        Map<String,String> supportedProperties = new HashMap<>();
        supportedProperties.put("storage_medium","SSD");
        supportedProperties.put("storage_cooldown_time","2022-07-09 12:12:12");
        supportedProperties.put("replication_num","1");
        supportedProperties.put("in_memory","false");
        if (properties != null){
            properties.forEach((k,v) -> supportedProperties.put(k,v));
        }
        return supportedProperties;
    }

    @Test
    public void testSingleListPartitionDesc() throws AnalysisException {
        ListPartitionDesc listPartitionDesc = this.findListSinglePartitionDesc("province",
                "p1","p2",null);
        listPartitionDesc.analyze(this.findColumnDefList(), null);
        String sql = listPartitionDesc.toSql();
        String target = "PARTITION BY LIST(`province`)(\n" +
                "    PARTITION p1 VALUES IN (\"guangdong\",\"tianjin\"),\n" +
                "    PARTITION p2 VALUES IN (\"shanghai\",\"beijing\")\n" +
                ")";
        Assert.assertEquals(sql, target);
    }

    @Test
    public void testMultiListPartitionDesc() throws AnalysisException {
        ListPartitionDesc listPartitionDesc = this.findListMultiPartitionDesc("dt,province",
                "p1","p2",null);
        listPartitionDesc.analyze(this.findColumnDefList(), null);
        String sql = listPartitionDesc.toSql();
        String target = "PARTITION BY LIST(`dt`,`province`)(\n" +
                "    PARTITION p1 VALUES IN ((\"2022-04-15\",\"guangdong\"),(\"2022-04-15\",\"tianjin\")),\n" +
                "    PARTITION p2 VALUES IN ((\"2022-04-16\",\"shanghai\"),(\"2022-04-16\",\"beijing\"))\n" +
                ")";
        Assert.assertEquals(sql, target);
    }

    @Test(expected = AnalysisException.class)
    public void testDuplicatePartitionColumn() throws AnalysisException{
        ListPartitionDesc listPartitionDesc = this.findListSinglePartitionDesc("dt,dt","p1","p1",null);
        listPartitionDesc.analyze(this.findColumnDefList(),null);
    }

    @Test(expected = AnalysisException.class)
    public void testNotAggregatedColumn() throws AnalysisException{
        ColumnDef province = new ColumnDef("province",TypeDef.createVarchar(64));
        province.setAggregateType(AggregateType.MAX);
        ColumnDef dt = new ColumnDef("dt",TypeDef.createVarchar(10));
        dt.setAggregateType(AggregateType.NONE);
        List<ColumnDef> columnDefList = Lists.newArrayList(province,dt);
        ListPartitionDesc listSinglePartitionDesc = this.findListSinglePartitionDesc("province","p1","p2",null);
        listSinglePartitionDesc.analyze(columnDefList,null);
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidDataType() throws AnalysisException{
        ListPartitionDesc listSinglePartitionDesc = this.findListSinglePartitionDesc("recharge_money","p1","p2",null);
        listSinglePartitionDesc.analyze(this.findColumnDefList(),null);
    }

    @Test(expected = AnalysisException.class)
    public void testColumnNoExist() throws AnalysisException{
        ListPartitionDesc listPartitionDesc = this.findListSinglePartitionDesc("name","p1","p1",null);
        listPartitionDesc.analyze(this.findColumnDefList(),null);
    }

    @Test(expected = AnalysisException.class)
    public void testDuplicateSingleListPartitionNames() throws AnalysisException {
        ListPartitionDesc listSinglePartitionDesc = this.findListSinglePartitionDesc("province","p1","p1",null);
        listSinglePartitionDesc.analyze(this.findColumnDefList(), null);
    }

    @Test(expected = AnalysisException.class)
    public void testDuplicateMultiListPartitionNames() throws AnalysisException {
        ListPartitionDesc listMultiPartitionDesc = this.findListMultiPartitionDesc("dt,province","p1","p1",null);
        listMultiPartitionDesc.analyze(this.findColumnDefList(), null);
    }

    @Test(expected = AnalysisException.class)
    public void testStorageMedium() throws AnalysisException {
        Map<String,String> supportedProperties = new HashMap<>();
        supportedProperties.put("storage_medium","xxx");
        ListPartitionDesc listMultiPartitionDesc = this.findListMultiPartitionDesc(
                "dt,province","p1","p1",this.findSupportedProperties(supportedProperties));
        listMultiPartitionDesc.analyze(this.findColumnDefList(), this.findSupportedProperties(null));
    }

    @Test(expected = AnalysisException.class)
    public void testCoolDownTime() throws AnalysisException {
        Map<String,String> supportedProperties = new HashMap<>();
        supportedProperties.put("storage_cooldown_time","2021-04-01 12:12:12");
        ListPartitionDesc listMultiPartitionDesc = this.findListMultiPartitionDesc(
                "dt,province","p1","p1",this.findSupportedProperties(supportedProperties));
        listMultiPartitionDesc.analyze(this.findColumnDefList(), this.findSupportedProperties(null));
    }

    @Test(expected = AnalysisException.class)
    public void testReplicaNum() throws AnalysisException {
        Map<String,String> supportedProperties = new HashMap<>();
        supportedProperties.put("replication_num","0");
        ListPartitionDesc listMultiPartitionDesc = this.findListMultiPartitionDesc(
                "dt,province","p1","p1",this.findSupportedProperties(supportedProperties));
        listMultiPartitionDesc.analyze(this.findColumnDefList(), this.findSupportedProperties(null));
    }

    @Test(expected = AnalysisException.class)
    public void testIsInMemory () throws AnalysisException {
        Map<String,String> supportedProperties = new HashMap<>();
        supportedProperties.put("in_memory","xxx");
        ListPartitionDesc listMultiPartitionDesc = this.findListMultiPartitionDesc(
                "dt,province","p1","p1",this.findSupportedProperties(supportedProperties));
        listMultiPartitionDesc.analyze(this.findColumnDefList(), this.findSupportedProperties(null));
    }

    @Test(expected = AnalysisException.class)
    public void testUnSupportProperties () throws AnalysisException {
        Map<String,String> supportedProperties = new HashMap<>();
        supportedProperties.put("no_support","xxx");
        ListPartitionDesc listMultiPartitionDesc = this.findListMultiPartitionDesc(
                "dt,province","p1","p1",this.findSupportedProperties(supportedProperties));
        listMultiPartitionDesc.analyze(this.findColumnDefList(), null);
    }

}
