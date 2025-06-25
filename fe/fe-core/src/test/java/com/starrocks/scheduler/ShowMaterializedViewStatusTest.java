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


package com.starrocks.scheduler;

import com.starrocks.qe.ShowMaterializedViewStatus;
import com.starrocks.thrift.TMaterializedViewStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ShowMaterializedViewStatusTest {

    @Test
    public void toThriftReturnsDefaultValuesWhenNoRefreshJobStatus() {
        ShowMaterializedViewStatus viewStatus = new ShowMaterializedViewStatus(1L, "testDb", "testView");

        TMaterializedViewStatus thriftStatus = viewStatus.toThrift();

        Assert.assertEquals("1", thriftStatus.getId());
        Assert.assertEquals("testDb", thriftStatus.getDatabase_name());
        Assert.assertEquals("testView", thriftStatus.getName());
        Assert.assertNull(thriftStatus.getRefresh_type());
        Assert.assertEquals("false", thriftStatus.getIs_active());
        Assert.assertNull(thriftStatus.getInactive_reason());
        Assert.assertNull(thriftStatus.getPartition_type());
        Assert.assertEquals("null", thriftStatus.getLast_refresh_state());
        Assert.assertEquals("\\N", thriftStatus.getLast_refresh_start_time());
        Assert.assertEquals("\\N", thriftStatus.getLast_refresh_process_time());
        Assert.assertNull(thriftStatus.getLast_refresh_finished_time());
        Assert.assertNull(thriftStatus.getLast_refresh_duration());
        Assert.assertNull(thriftStatus.getLast_refresh_error_code());
        Assert.assertNull(thriftStatus.getLast_refresh_error_message());
        Assert.assertEquals("", thriftStatus.getExtra_message());
        Assert.assertEquals("0", thriftStatus.getRows());
        Assert.assertNull(thriftStatus.getQuery_rewrite_status());
        Assert.assertNull(thriftStatus.getCreator());
    }

    @Test
    public void toResultSetReturnsEmptyFieldsWhenNoRefreshJobStatus() {
        ShowMaterializedViewStatus viewStatus = new ShowMaterializedViewStatus(1L, "testDb", "testView");

        List<String> resultSet = viewStatus.toResultSet();

        Assert.assertEquals("", resultSet.get(3)); // refresh type
        Assert.assertEquals("false", resultSet.get(4)); // is active
        Assert.assertEquals("", resultSet.get(5)); // inactive reason
        Assert.assertEquals("", resultSet.get(6)); // partition type
        Assert.assertEquals("0", resultSet.get(7)); // task id
        Assert.assertEquals("", resultSet.get(8)); // task name
        Assert.assertEquals("\\N", resultSet.get(9)); // start time
        Assert.assertEquals("\\N", resultSet.get(10)); // process start time
        Assert.assertEquals("\\N", resultSet.get(11)); // process finish time
        Assert.assertEquals("0.000", resultSet.get(12)); // process duration
        Assert.assertEquals("", resultSet.get(13)); // last refresh job id
        Assert.assertEquals("", resultSet.get(14)); // last refresh state
        Assert.assertEquals("false", resultSet.get(15)); // force refresh
        Assert.assertEquals("", resultSet.get(16)); // partition start
        Assert.assertEquals("", resultSet.get(17)); // partition end
        Assert.assertEquals("", resultSet.get(18)); // base partitions
        Assert.assertEquals("", resultSet.get(19)); // mv partitions
        Assert.assertEquals("", resultSet.get(20)); // error code
        Assert.assertEquals("", resultSet.get(21)); // error message
        Assert.assertEquals("0", resultSet.get(22)); // extra message
        Assert.assertEquals("", resultSet.get(23)); // query rewrite status
        Assert.assertEquals("", resultSet.get(24)); // owner
    }
}