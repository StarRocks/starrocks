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

package com.starrocks.load;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.common.FeConstants;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.proc.LoadProcDir;
import com.starrocks.load.FailMsg.CancelType;
import com.starrocks.load.routineload.ErrorReason;
import com.starrocks.sql.ast.ShowRoutineLoadStmt;
import com.starrocks.sql.ast.ShowStreamLoadStmt;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TRoutineLoadJobInfo;
import com.starrocks.thrift.TStreamLoadInfo;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LoadJobRedactionTest {
    private static final String URL = "http://127.0.0.1:8040/api/_load_error_log?file=error_log_x";
    private static final String TRACKING_SQL = "select tracking_log from information_schema.load_tracking_logs";
    // A rejected value, as an error message can quote it.
    private static final String ROW_VALUE = "ssn 123-45-6789";

    @Test
    public void testLoadInfo() {
        FailMsg failMsg = new FailMsg(CancelType.ETL_QUALITY_UNSATISFIED, ROW_VALUE);
        TLoadInfo info = loadInfo("type:" + CancelType.ETL_QUALITY_UNSATISFIED + "; msg:" + ROW_VALUE);

        LoadJobRedaction.redact(info, failMsg);

        assertEquals("type:ETL_QUALITY_UNSATISFIED", info.getError_msg());
        assertFalse(info.isSetUrl());
        assertFalse(info.isSetTracking_sql());
        assertFalse(info.isSetRejected_record_path());
        assertFalse(info.isSetProperties());
        JsonObject details = JsonParser.parseString(info.getRuntime_details()).getAsJsonObject();
        assertEquals(Set.of("txn_id", "file_size"), details.keySet());
        // Numbers are written back as they were.
        assertEquals("{\"file_size\":10,\"txn_id\":1001}", info.getRuntime_details());
        // The metadata stays.
        assertEquals("label", info.getLabel());
        assertEquals(3, info.getNum_filtered_rows());
    }

    @Test
    public void testLoadInfoWithoutFailureType() {
        // A stream load's error is free text: it has no type to keep.
        TLoadInfo info = loadInfo(ROW_VALUE);
        LoadJobRedaction.redact(info, null);
        assertFalse(info.isSetError_msg());

        // A job that has not failed has no error to show.
        info = loadInfo(null);
        LoadJobRedaction.redact(info, new FailMsg(CancelType.LOAD_RUN_FAIL, ROW_VALUE));
        assertFalse(info.isSetError_msg());
    }

    @Test
    public void testStreamLoadInfo() {
        TStreamLoadInfo info = new TStreamLoadInfo();
        info.setLabel("label");
        info.setLoad_id("load-id");
        info.setError_msg(ROW_VALUE);
        info.setTracking_url(URL);
        info.setTracking_sql(TRACKING_SQL);

        LoadJobRedaction.redact(info);

        assertFalse(info.isSetError_msg());
        assertFalse(info.isSetTracking_url());
        assertFalse(info.isSetTracking_sql());
        assertEquals("label", info.getLabel());
    }

    @Test
    public void testRoutineLoadJobInfo() {
        TRoutineLoadJobInfo info = routineLoadJobInfo(
                new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR, ROW_VALUE).toString());

        LoadJobRedaction.redact(info);

        assertEquals("ErrorReason{errCode = 102}", info.getReasons_of_state_changed());
        assertFalse(info.isSetJob_properties());
        assertFalse(info.isSetData_source_properties());
        assertFalse(info.isSetCustom_properties());
        assertFalse(info.isSetError_log_urls());
        assertFalse(info.isSetTracking_sql());
        assertFalse(info.isSetOther_msg());
        assertEquals("job", info.getName());

        // A running job has no reason, which is kept as it is.
        info = routineLoadJobInfo("");
        LoadJobRedaction.redact(info);
        assertEquals("", info.getReasons_of_state_changed());
        // A reason of an unknown form is left out whole.
        info = routineLoadJobInfo(ROW_VALUE);
        LoadJobRedaction.redact(info);
        assertFalse(info.isSetReasons_of_state_changed());
    }

    @Test
    public void testReasonWithoutMessage() {
        assertEquals("ErrorReason{errCode = 5004}",
                ErrorReason.withoutMessage(new ErrorReason(5004, "msg', with ', msg='").toString()));
        assertNull(ErrorReason.withoutMessage(ROW_VALUE));
    }

    @Test
    public void testLoadInfoApi() {
        Load.JobInfo info = new Load.JobInfo("db", "label");
        info.failMsg = ROW_VALUE;
        info.trackingUrl = URL;
        LoadJobRedaction.redact(info, new FailMsg(CancelType.LOAD_RUN_FAIL, ROW_VALUE));
        assertEquals("type:LOAD_RUN_FAIL", info.failMsg);
        assertEquals(EtlStatus.DEFAULT_TRACKING_URL, info.trackingUrl);

        info.failMsg = "";
        LoadJobRedaction.redact(info, null);
        assertEquals("", info.failMsg);
    }

    @Test
    public void testShowLoad() {
        List<String> titles = LoadProcDir.TITLE_NAMES;
        List<Comparable> row = new ArrayList<>();
        titles.forEach(title -> row.add(title));
        row.set(titles.indexOf("TaskInfo"), "resource:N/A; timeout(s):300; max_filter_ratio:0.0");
        row.set(titles.indexOf("ErrorMsg"), "type:LOAD_RUN_FAIL; msg:" + ROW_VALUE);
        row.set(titles.indexOf("TrackingSQL"), TRACKING_SQL);
        row.set(titles.indexOf("JobDetails"), "{\"All backends\":{\"load-id\":[10001]},\"FileNumber\":0,"
                + "\"ScanRows\":7,\"Unfinished backends\":{\"load-id\":[]}}");

        List<Comparable> redacted = LoadJobRedaction.redactShowLoad(row,
                new FailMsg(CancelType.LOAD_RUN_FAIL, ROW_VALUE));

        assertEquals(FeConstants.NULL_STRING, redacted.get(titles.indexOf("TaskInfo")));
        assertEquals("type:LOAD_RUN_FAIL", redacted.get(titles.indexOf("ErrorMsg")));
        assertEquals(FeConstants.NULL_STRING, redacted.get(titles.indexOf("TrackingSQL")));
        assertEquals("{\"FileNumber\":0,\"ScanRows\":7}", redacted.get(titles.indexOf("JobDetails")));
        assertEquals("Label", redacted.get(titles.indexOf("Label")));
        // The row given is left as it is.
        assertEquals(TRACKING_SQL, row.get(titles.indexOf("TrackingSQL")));

        redacted = LoadJobRedaction.redactShowLoad(row, null);
        assertEquals(FeConstants.NULL_STRING, redacted.get(titles.indexOf("ErrorMsg")));
    }

    @Test
    public void testShowStreamLoad() {
        List<String> titles = ShowStreamLoadStmt.getTitleNames();
        List<String> redacted = LoadJobRedaction.redactShowStreamLoad(new ArrayList<>(titles));
        for (String title : titles) {
            boolean removed = List.of("ErrorMsg", "TrackingURL", "TrackingSQL").contains(title);
            assertEquals(removed ? null : title, redacted.get(titles.indexOf(title)), title);
        }
    }

    @Test
    public void testShowRoutineLoad() {
        List<String> titles = ShowRoutineLoadStmt.getTitleNames();
        List<String> row = new ArrayList<>(titles);
        row.set(titles.indexOf("ReasonOfStateChanged"),
                new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR, ROW_VALUE).toString());

        List<String> redacted = LoadJobRedaction.redactShowRoutineLoad(row);

        List<String> removed = List.of("JobProperties", "DataSourceProperties", "CustomProperties", "ErrorLogUrls",
                "TrackingSQL", "OtherMsg");
        for (String title : titles) {
            if (title.equals("ReasonOfStateChanged")) {
                assertEquals("ErrorReason{errCode = 102}", redacted.get(titles.indexOf(title)));
            } else {
                assertEquals(removed.contains(title) ? null : title, redacted.get(titles.indexOf(title)), title);
            }
        }
    }

    private static TLoadInfo loadInfo(String errorMsg) {
        TLoadInfo info = new TLoadInfo();
        info.setLabel("label");
        info.setNum_filtered_rows(3);
        if (errorMsg != null) {
            info.setError_msg(errorMsg);
        }
        info.setUrl(URL);
        info.setTracking_sql(TRACKING_SQL);
        info.setRejected_record_path("127.0.0.1:/home/disk1/rejected_record/x");
        info.setProperties("{\"max_filter_ratio\":0.0,\"timeout\":300}");
        info.setRuntime_details("{\"backends\":{\"load-id\":[10001]},\"file_size\":10,\"load_id\":\"load-id\","
                + "\"txn_error_msg\":\"" + ROW_VALUE + "\",\"txn_id\":1001,\"unfinished_backends\":{\"load-id\":[]}}");
        return info;
    }

    private static TRoutineLoadJobInfo routineLoadJobInfo(String reason) {
        TRoutineLoadJobInfo info = new TRoutineLoadJobInfo();
        info.setName("job");
        info.setJob_properties("{\"max_error_number\":\"0\"}");
        info.setData_source_properties("{\"topic\":\"t\"}");
        info.setCustom_properties("{}");
        info.setReasons_of_state_changed(reason);
        info.setError_log_urls(URL);
        info.setTracking_sql(TRACKING_SQL);
        info.setOther_msg(ROW_VALUE);
        return info;
    }
}
