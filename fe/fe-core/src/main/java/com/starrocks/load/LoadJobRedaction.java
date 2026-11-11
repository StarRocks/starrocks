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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.starrocks.common.FeConstants;
import com.starrocks.common.proc.LoadProcDir;
import com.starrocks.load.routineload.ErrorReason;
import com.starrocks.sql.ast.ShowRoutineLoadStmt;
import com.starrocks.sql.ast.ShowStreamLoadStmt;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TRoutineLoadJobInfo;
import com.starrocks.thrift.TStreamLoadInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Removes from the output of a load job what can reveal the rows it loads, for a caller that may see the job but
 * does not hold SELECT on every table it loads into ({@link LoadJobVisibility.Access#METADATA}). Removed, that is
 * NULL, are the error-log URL and the tracking SQL that reads it, the rejected record path, the error messages, the
 * properties, and of the runtime details the load ids, the backends keyed by them and the transaction error. A failed
 * load or routine load keeps its failure type, without the message.
 */
public final class LoadJobRedaction {

    // The entries of the runtime details (information_schema.loads) and of the job details (SHOW LOAD) holding a load
    // id or an error message.
    private static final List<String> RUNTIME_DETAILS_KEYS = List.of(LoadConstants.RUNTIME_DETAILS_LOAD_ID,
            LoadConstants.RUNTIME_DETAILS_TXN_ERROR_MSG, LoadConstants.RUNTIME_DETAILS_UNFINISHED_BACKENDS,
            LoadConstants.RUNTIME_DETAILS_BACKENDS);
    private static final List<String> JOB_DETAILS_KEYS = List.of("Unfinished backends", "All backends");

    private static final int LOAD_TASK_INFO = columnOf(LoadProcDir.TITLE_NAMES, "TaskInfo");
    private static final int LOAD_ERROR_MSG = columnOf(LoadProcDir.TITLE_NAMES, "ErrorMsg");
    private static final int LOAD_TRACKING_SQL = columnOf(LoadProcDir.TITLE_NAMES, "TrackingSQL");
    private static final int LOAD_JOB_DETAILS = columnOf(LoadProcDir.TITLE_NAMES, "JobDetails");

    private static final List<Integer> STREAM_LOAD_NULL_COLUMNS = List.of(
            columnOf(ShowStreamLoadStmt.getTitleNames(), "ErrorMsg"),
            columnOf(ShowStreamLoadStmt.getTitleNames(), "TrackingURL"),
            columnOf(ShowStreamLoadStmt.getTitleNames(), "TrackingSQL"));

    private static final int ROUTINE_LOAD_REASON = columnOf(ShowRoutineLoadStmt.getTitleNames(),
            "ReasonOfStateChanged");
    private static final List<Integer> ROUTINE_LOAD_NULL_COLUMNS = List.of(
            columnOf(ShowRoutineLoadStmt.getTitleNames(), "JobProperties"),
            columnOf(ShowRoutineLoadStmt.getTitleNames(), "DataSourceProperties"),
            columnOf(ShowRoutineLoadStmt.getTitleNames(), "CustomProperties"),
            columnOf(ShowRoutineLoadStmt.getTitleNames(), "ErrorLogUrls"),
            columnOf(ShowRoutineLoadStmt.getTitleNames(), "TrackingSQL"),
            columnOf(ShowRoutineLoadStmt.getTitleNames(), "OtherMsg"));

    private LoadJobRedaction() {
    }

    /**
     * Redacts a row of information_schema.loads. {@code failMsg} is that of the load job the row is of, null for a
     * stream load, whose error has no type.
     */
    public static TLoadInfo redact(TLoadInfo info, FailMsg failMsg) {
        if (info.isSetError_msg() && failMsg != null) {
            info.setError_msg(failureType(failMsg));
        } else {
            info.unsetError_msg();
        }
        info.unsetUrl();
        info.unsetTracking_sql();
        info.unsetRejected_record_path();
        info.unsetProperties();
        if (info.isSetRuntime_details()) {
            info.setRuntime_details(withoutEntries(info.getRuntime_details(), RUNTIME_DETAILS_KEYS));
        }
        return info;
    }

    /** Redacts a row of information_schema.stream_loads. */
    public static TStreamLoadInfo redact(TStreamLoadInfo info) {
        info.unsetError_msg();
        info.unsetTracking_url();
        info.unsetTracking_sql();
        return info;
    }

    /** Redacts a row of information_schema.routine_load_jobs. */
    public static TRoutineLoadJobInfo redact(TRoutineLoadJobInfo info) {
        info.unsetJob_properties();
        info.unsetData_source_properties();
        info.unsetCustom_properties();
        if (info.isSetReasons_of_state_changed()) {
            String reason = reasonWithoutMessage(info.getReasons_of_state_changed());
            if (reason == null) {
                info.unsetReasons_of_state_changed();
            } else {
                info.setReasons_of_state_changed(reason);
            }
        }
        info.unsetError_log_urls();
        info.unsetTracking_sql();
        info.unsetOther_msg();
        return info;
    }

    /** Redacts the load info API's answer on a load job whose fail message is {@code failMsg}. */
    public static Load.JobInfo redact(Load.JobInfo info, FailMsg failMsg) {
        info.failMsg = failMsg == null ? "" : failureType(failMsg);
        info.trackingUrl = EtlStatus.DEFAULT_TRACKING_URL;
        return info;
    }

    /** Redacts a row of SHOW LOAD or SHOW PROC '/jobs/db/load' on a load job whose fail message is {@code failMsg}. */
    public static List<Comparable> redactShowLoad(List<Comparable> row, FailMsg failMsg) {
        List<Comparable> redacted = new ArrayList<>(row);
        redacted.set(LOAD_TASK_INFO, FeConstants.NULL_STRING);
        redacted.set(LOAD_ERROR_MSG, failMsg == null ? FeConstants.NULL_STRING : failureType(failMsg));
        redacted.set(LOAD_TRACKING_SQL, FeConstants.NULL_STRING);
        String jobDetails = withoutEntries(row.get(LOAD_JOB_DETAILS).toString(), JOB_DETAILS_KEYS);
        redacted.set(LOAD_JOB_DETAILS, jobDetails == null ? FeConstants.NULL_STRING : jobDetails);
        return redacted;
    }

    /** Redacts a row of SHOW STREAM LOAD or SHOW PROC '/stream_loads/label'. */
    public static List<String> redactShowStreamLoad(List<String> row) {
        List<String> redacted = new ArrayList<>(row);
        STREAM_LOAD_NULL_COLUMNS.forEach(column -> redacted.set(column, null));
        return redacted;
    }

    /** Redacts a row of SHOW ROUTINE LOAD or SHOW PROC '/routine_loads/name'. */
    public static List<String> redactShowRoutineLoad(List<String> row) {
        List<String> redacted = new ArrayList<>(row);
        redacted.set(ROUTINE_LOAD_REASON, reasonWithoutMessage(row.get(ROUTINE_LOAD_REASON)));
        ROUTINE_LOAD_NULL_COLUMNS.forEach(column -> redacted.set(column, null));
        return redacted;
    }

    // The error message of a failed load job ("type:<type>; msg:<message>") without the message.
    private static String failureType(FailMsg failMsg) {
        return "type:" + failMsg.getCancelType();
    }

    // An empty reason, given while the job runs, says nothing and is kept.
    private static String reasonWithoutMessage(String reason) {
        return reason == null || reason.isEmpty() ? reason : ErrorReason.withoutMessage(reason);
    }

    private static String withoutEntries(String json, List<String> keys) {
        try {
            JsonElement element = JsonParser.parseString(json);
            if (!element.isJsonObject()) {
                return null;
            }
            JsonObject object = element.getAsJsonObject();
            keys.forEach(object::remove);
            return object.toString();
        } catch (JsonParseException e) {
            return null;
        }
    }

    private static int columnOf(List<String> titles, String title) {
        int column = titles.indexOf(title);
        if (column < 0) {
            throw new IllegalStateException("No column " + title + " in " + titles);
        }
        return column;
    }
}
