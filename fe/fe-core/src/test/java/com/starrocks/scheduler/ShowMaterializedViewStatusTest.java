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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ShowMaterializedViewStatusTest {

    @Test
    public void toThriftReturnsDefaultValuesWhenNoRefreshJobStatus() {
        ShowMaterializedViewStatus viewStatus = new ShowMaterializedViewStatus(1L, "testDb", "testView");

        TMaterializedViewStatus thriftStatus = viewStatus.toThrift();

        Assertions.assertEquals("1", thriftStatus.getId());
        Assertions.assertEquals("testDb", thriftStatus.getDatabase_name());
        Assertions.assertEquals("testView", thriftStatus.getName());
        Assertions.assertNull(thriftStatus.getRefresh_type());
        Assertions.assertEquals("false", thriftStatus.getIs_active());
        Assertions.assertNull(thriftStatus.getInactive_reason());
        Assertions.assertNull(thriftStatus.getPartition_type());
        Assertions.assertEquals("null", thriftStatus.getLast_refresh_state());
        Assertions.assertEquals("\\N", thriftStatus.getLast_refresh_start_time());
        Assertions.assertEquals("\\N", thriftStatus.getLast_refresh_process_time());
        Assertions.assertNull(thriftStatus.getLast_refresh_finished_time());
        Assertions.assertNull(thriftStatus.getLast_refresh_duration());
        Assertions.assertNull(thriftStatus.getLast_refresh_error_code());
        Assertions.assertNull(thriftStatus.getLast_refresh_error_message());
        Assertions.assertEquals("", thriftStatus.getExtra_message());
        Assertions.assertEquals("0", thriftStatus.getRows());
        Assertions.assertNull(thriftStatus.getQuery_rewrite_status());
        Assertions.assertNull(thriftStatus.getCreator());
    }

    @Test
    public void toResultSetReturnsEmptyFieldsWhenNoRefreshJobStatus() {
        ShowMaterializedViewStatus viewStatus = new ShowMaterializedViewStatus(1L, "testDb", "testView");

        List<String> resultSet = viewStatus.toResultSet();

        Assertions.assertEquals("", resultSet.get(3)); // refresh type
        Assertions.assertEquals("false", resultSet.get(4)); // is active
        Assertions.assertEquals("", resultSet.get(5)); // inactive reason
        Assertions.assertEquals("", resultSet.get(6)); // partition type
        Assertions.assertEquals("0", resultSet.get(7)); // task id
        Assertions.assertEquals("", resultSet.get(8)); // task name
        Assertions.assertEquals("\\N", resultSet.get(9)); // start time
        Assertions.assertEquals("\\N", resultSet.get(10)); // process finish time
        Assertions.assertEquals("0.000", resultSet.get(11)); // process duration
        Assertions.assertEquals("", resultSet.get(12)); // last refresh state
        Assertions.assertEquals("false", resultSet.get(13)); // force refresh
        Assertions.assertEquals("", resultSet.get(14)); // partition start
        Assertions.assertEquals("", resultSet.get(15)); // partition end
        Assertions.assertEquals("", resultSet.get(16)); // base partitions
        Assertions.assertEquals("", resultSet.get(17)); // mv partitions
        Assertions.assertEquals("", resultSet.get(18)); // error code
        Assertions.assertEquals("", resultSet.get(19)); // error message
        Assertions.assertEquals("0", resultSet.get(20)); // extra message
        Assertions.assertEquals("", resultSet.get(21)); // query rewrite status
        Assertions.assertEquals("", resultSet.get(22)); // owner
        Assertions.assertEquals("", resultSet.get(23)); // process start time
        Assertions.assertEquals("", resultSet.get(24)); // last refresh job id
    }
}