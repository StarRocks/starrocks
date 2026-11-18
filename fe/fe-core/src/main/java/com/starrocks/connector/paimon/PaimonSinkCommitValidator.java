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

package com.starrocks.connector.paimon;

import com.google.common.base.Preconditions;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.thrift.TPaimonCommitInfo;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Validates the native prepare protocol against the actual scheduled sink drivers. */
public final class PaimonSinkCommitValidator {
    private final PaimonSinkExtra operation;
    private final Map<TUniqueId, Set<String>> expectedProducers = new HashMap<>();
    private final Set<TUniqueId> finishedInstances = new HashSet<>();
    private final Map<String, Long> producerRows = new HashMap<>();
    private final RuntimeProfile profile;
    private String reportFailure;

    public PaimonSinkCommitValidator(PaimonSinkExtra operation, Map<TUniqueId, Integer> sinkInstanceDops,
                                     RuntimeProfile profile) {
        Preconditions.checkArgument(operation != null && !sinkInstanceDops.isEmpty(),
                "Paimon publication requires scheduled sink producers");
        this.operation = operation;
        this.profile = profile;
        int producerCount = 0;
        for (Map.Entry<TUniqueId, Integer> entry : sinkInstanceDops.entrySet()) {
            int dop = entry.getValue();
            if (entry.getKey() == null || dop <= 0 || dop > 100_000 - producerCount) {
                fail("Invalid Paimon scheduled producer layout");
            }
            Set<String> producers = new HashSet<>();
            for (int driver = 0; driver < dop; driver++) {
                producers.add(DebugUtil.printId(entry.getKey()) + ":" + driver);
            }
            expectedProducers.put(entry.getKey().deepCopy(), producers);
            producerCount += dop;
        }
        profile.addInfoString("PaimonExpectedProducers", Integer.toString(producerCount));
    }

    public synchronized void recordAggregationFailure() {
        reportFailure = "Cannot aggregate the final Paimon producer report";
    }

    public synchronized void validateComplete(List<TSinkCommitInfo> commitInfos, long loadedRows) {
        if (reportFailure != null) {
            throw new StarRocksConnectorException(reportFailure);
        }
        if (!finishedInstances.equals(expectedProducers.keySet())) {
            fail("Missing final Paimon sink instance reports");
        }
        long reportedRows = 0;
        try {
            for (long rows : producerRows.values()) {
                reportedRows = Math.addExact(reportedRows, rows);
            }
        } catch (ArithmeticException e) {
            fail("Paimon producer row count overflow");
        }
        if (reportedRows != loadedRows) {
            fail("Paimon producer row counts do not match the completed load");
        }
        // Check the actual submission, including zero-row producers, before decoding native payloads.
        PaimonCommitCodec.validateReportCount(commitInfos);
        Set<String> submitted = new HashSet<>();
        for (TSinkCommitInfo commitInfo : commitInfos) {
            if (commitInfo == null || !commitInfo.isSetPaimon_commit_info()) {
                throw new StarRocksConnectorException("Missing native Paimon prepare marker");
            }
            TPaimonCommitInfo info = commitInfo.getPaimon_commit_info();
            if (!info.isSetAttempt_id() || info.getAttempt_id() != 0 || !info.isSetRow_count()
                    || !Objects.equals(producerRows.get(info.getProducer_id()), info.getRow_count())) {
                throw new StarRocksConnectorException("Unexpected Paimon producer in the commit payload");
            }
            submitted.add(info.getProducer_id());
        }
        if (submitted.size() != producerRows.size()) {
            throw new StarRocksConnectorException("Missing Paimon sink driver prepare markers in the commit payload");
        }
        profile.addInfoString("PaimonPreparedProducers", Integer.toString(producerRows.size()));
    }

    public synchronized void validateReport(TUniqueId instanceId, TReportExecStatusParams params) {
        List<TSinkCommitInfo> reports = params.isSetSink_commit_infos() ? params.getSink_commit_infos() : List.of();
        Set<String> expected = expectedProducers.get(instanceId);
        if (expected == null) {
            if (!reports.isEmpty()) {
                fail("Unexpected Paimon producer report from a source-only fragment");
            }
            return;
        }
        if (!instanceId.equals(params.getFragment_instance_id())) {
            fail("Mismatched Paimon sink instance report");
        }
        if (!params.isSetStatus() || !params.getStatus().isSetStatus_code()) {
            fail("Missing final Paimon sink execution status");
        }
        // Failed instances retain abort payloads without satisfying publication completeness.
        if (params.getStatus().getStatus_code() != TStatusCode.OK) {
            reportFailure = "A Paimon sink instance failed before completing prepare";
            return;
        }
        Set<String> reported = new HashSet<>();
        for (TSinkCommitInfo report : reports) {
            if (report == null || !report.isSetPaimon_commit_info()) {
                fail("Missing native Paimon prepare marker");
            }
            TPaimonCommitInfo info = report.getPaimon_commit_info();
            if (!info.isSetProducer_id() || !expected.contains(info.getProducer_id())
                    || !info.isSetAttempt_id() || info.getAttempt_id() != 0
                    || !info.isSetPrepare_complete() || !info.isPrepare_complete()
                    || !info.isSetOperation_id() || !operation.getOperationId().equals(info.getOperation_id())
                    || !info.isSetSchema_id() || operation.getSchemaId() != info.getSchema_id()
                    || !info.isSetTable_identity() || !operation.getTableIdentity().equals(info.getTable_identity())
                    || !info.isSetSerializer_version() || !info.isSetCommit_messages()
                    || !info.isSetRow_count() || info.getRow_count() < 0) {
                fail("Incomplete or unexpected Paimon prepare marker");
            }
            reported.add(info.getProducer_id());
            Long previousRows = producerRows.putIfAbsent(info.getProducer_id(), info.getRow_count());
            if (previousRows != null && previousRows != info.getRow_count()) {
                fail("Conflicting Paimon producer row counts");
            }
        }
        if (!reported.equals(expected)) {
            fail("Missing Paimon sink driver prepare markers");
        }
        finishedInstances.add(instanceId);
    }

    private void fail(String message) {
        reportFailure = message;
        throw new StarRocksConnectorException(message);
    }
}
