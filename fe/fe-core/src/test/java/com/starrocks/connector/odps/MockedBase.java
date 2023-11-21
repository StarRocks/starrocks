package com.starrocks.connector.odps;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tables;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.impl.batch.TableBatchReadSessionImpl;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableList;
import com.starrocks.credential.aliyun.AliyunCloudCredential;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class MockedBase {
    protected AliyunCloudCredential aliyunCloudCredential = new AliyunCloudCredential("ak", "sk", "http://127.0.0.1");
    protected OdpsProperties odpsProperties;
    protected Odps odps = Mockito.mock(Odps.class);
    protected Tables tables = Mockito.mock(Tables.class);
    protected Partition partition = Mockito.mock(Partition.class);
    protected Iterator<Table> tableIterator = Mockito.mock(Iterator.class);
    protected Table table = Mockito.mock(Table.class);
    protected Account account = Mockito.mock(AliyunAccount.class);

    @Mock
    TableReadSessionBuilder mockTableReadSessionBuilder = Mockito.mock(TableReadSessionBuilder.class);
    TableBatchReadSession tableBatchReadSession = Mockito.mock(TableBatchReadSessionImpl.class);
    InputSplitAssigner inputSplitAssigner = Mockito.mock(IndexedInputSplitAssigner.class);

    public void initMock() throws OdpsException, IOException {
        Map<String, String> properties = new HashMap<>();
        properties.put("odps.access.id", "ak");
        properties.put("odps.access.key", "sk");
        properties.put("odps.endpoint", "http://127.0.0.1");
        properties.put("odps.project", "project");
        odpsProperties = new OdpsProperties(properties);

        when(odps.getEndpoint()).thenReturn("http://127.0.0.1");
        when(odps.getDefaultProject()).thenReturn("default_project");
        when(odps.tables()).thenReturn(tables);
        when(odps.getAccount()).thenReturn(account);

        when(tables.iterator(anyString())).thenReturn(tableIterator);
        when(tables.get(anyString(), anyString())).thenReturn(table);

        when(tableIterator.hasNext()).thenReturn(true, false);
        when(tableIterator.next()).thenReturn(table);

        when(table.getName()).thenReturn("tableName");
        when(table.getCreatedTime()).thenReturn(new Date());
        when(table.getProject()).thenReturn("project");
        doNothing().when(table).reload();
        when(table.getPartitions()).thenReturn(ImmutableList.of(partition));

        when(partition.getPartitionSpec()).thenReturn(new PartitionSpec("p1=a/p2=b"));
        when(partition.getLastDataModifiedTime()).thenReturn(new Date());

        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("c1", TypeInfoFactory.STRING));
        tableSchema.addColumn(new Column("c2", TypeInfoFactory.BIGINT));
        tableSchema.addPartitionColumn(new Column("p1", TypeInfoFactory.STRING));
        tableSchema.addPartitionColumn(new Column("p2", TypeInfoFactory.STRING));
        when(table.getSchema()).thenReturn(tableSchema);

        when(mockTableReadSessionBuilder.identifier(any())).thenReturn(mockTableReadSessionBuilder);
        when(mockTableReadSessionBuilder.withSettings(any())).thenReturn(mockTableReadSessionBuilder);
        when(mockTableReadSessionBuilder.requiredPartitions(any())).thenReturn(mockTableReadSessionBuilder);
        when(mockTableReadSessionBuilder.requiredDataColumns(any())).thenReturn(mockTableReadSessionBuilder);
        when(mockTableReadSessionBuilder.withSplitOptions(any())).thenReturn(mockTableReadSessionBuilder);

        when(mockTableReadSessionBuilder.buildBatchReadSession()).thenReturn(tableBatchReadSession);
        when(tableBatchReadSession.getInputSplitAssigner()).thenReturn(inputSplitAssigner);

        InputSplit split0 = new IndexedInputSplit("session", 0);
        InputSplit split1 = new IndexedInputSplit("session", 1);

        when(inputSplitAssigner.getAllSplits()).thenReturn(new InputSplit[] {split0, split1});
        when(inputSplitAssigner.getSplit(0)).thenReturn(split0);
        when(inputSplitAssigner.getSplit(1)).thenReturn(split1);
        when(inputSplitAssigner.getSplitsCount()).thenReturn(2);

        InputSplit split2 = new RowRangeInputSplit("session", 0, 10000L);
        when(inputSplitAssigner.getTotalRowCount()).thenReturn(10000L);
        when(inputSplitAssigner.getSplitByRowOffset(0, 10000L)).thenReturn(split2);
    }

    @Test
    public void test() {
        Iterator<Table> iterator = odps.tables().iterator("project");
        while (iterator.hasNext()) {
            Table table = iterator.next();
            System.out.println(table.getName());
        }
    }
}
