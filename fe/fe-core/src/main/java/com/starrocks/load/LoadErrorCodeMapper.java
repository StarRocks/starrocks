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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.DataQualityException;
import com.starrocks.load.loadv2.LoadErrorUtils;
import com.starrocks.transaction.TransactionCommitFailedException;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Derives a {@link LoadErrorCode} from what the FE knows about a failed load.
 *
 * <p>The structured cancel type of a {@link FailMsg} is consulted first; only
 * when it does not pin down a reason is the free-text message matched against
 * {@link #RULES}, an ordered table of (pattern, code). The first matching rule
 * wins, so the more specific families come first and the generic ones come last.
 * Generic words such as "timeout" or "conflict" only count next to a load,
 * transaction or publish context, so that a message quoting a file path or a
 * table name is not pulled into the wrong family. Matching is cheap and is meant
 * to run once per failed load, lazily, when its error code is first read.
 */
public final class LoadErrorCodeMapper {

    private LoadErrorCodeMapper() {
    }

    /**
     * An error code together with the message object it was derived from. Holders keep one
     * instance in a volatile field and recompute only when the message object changes, so a
     * reader never observes a code that belongs to a different message.
     */
    public record CachedCode(Object source, LoadErrorCode code) {
    }

    private record Rule(Pattern pattern, LoadErrorCode code) {
    }

    private static Rule rule(String regex, LoadErrorCode code) {
        return new Rule(Pattern.compile(regex, Pattern.CASE_INSENSITIVE), code);
    }

    // Object storage and file system URL schemes that appear in source paths.
    private static final String STORAGE_SCHEME = "(s3a?|oss|cos|obs|hdfs|gs|abfss?|wasbs?|tos|ks3|azblob)://";
    // A refused, reset or failed connection, whoever the peer is.
    private static final String CONNECTION_FAILURE = "(Connection (refused|reset)|Connect .* failed)";
    // What tells a source-side peer (broker, storage path, message system) from a backend.
    private static final String SOURCE_PEER = "(Broker (list path|open reader|read|open writer|write|delete path)|"
            + STORAGE_SCHEME + "|\\b(kafka|pulsar)\\b)";

    // Message families, matched in order. The patterns are taken from the messages
    // the FE produces itself (load, transaction, lake and scheduler packages) and from
    // the BE messages that come back through the coordinator or the transaction abort
    // reason (mem_tracker, stream load, tablet sink, brpc).
    private static final List<Rule> RULES = ImmutableList.of(
            // LabelAlreadyUsedException, transactional stream load begin.
            rule("Label \\[.*\\] has already been used|Transaction already (exists|started) with label",
                    LoadErrorCode.LABEL_ALREADY_EXISTS),
            // DatabaseTransactionMgr.beginTransaction.
            rule("current running txns on db|max_running_txn_num_per_db", LoadErrorCode.TOO_MANY_RUNNING_TXNS),
            // CommitRateLimiter: delayed commit, rejected commit and the ingestion-slowdown timeout.
            // Before TIMEOUT so that "timed out due to ingestion slowdown" lands here.
            rule("compaction score|lake_compaction_score_upper_bound|ingest(ion)? slowdown",
                    LoadErrorCode.COMPACTION_SCORE_EXCEEDED),
            // ETL quality check, the routine load FILTERED_ROWS txn reason, the INSERT abort reason,
            // the broker/spark load quality message and the merge commit quality check.
            rule("too many filtered rows|max_filter_ratio"
                    + "|" + Pattern.quote(TransactionCommitFailedException.FILTER_DATA_ERR)
                    + "|" + Pattern.quote(DataQualityException.QUALITY_FAIL_MSG)
                    + "|There is a data quality issue",
                    LoadErrorCode.FILTER_RATIO_EXCEEDED),
            // BE mem_tracker and delta writer messages.
            rule("Memory of .* exceed limit|Mem usage has exceed the limit|memory limit exceeded"
                    + "|exceed(ed|s)? .*mem(ory)? limit|out of memory|OutOfMemory|MEM_LIMIT_EXCEEDED",
                    LoadErrorCode.MEM_LIMIT_EXCEEDED),
            // CANCEL LOAD, stream load abort through the HTTP API, ROLLBACK, routine load pause/stop,
            // KILL of the INSERT's connection or query, and a client that went away.
            rule("user (cancel|abort)|cancell?ed by (the )?(user|client)|rollback transaction by user"
                    + "|(paused|stopped) by user|manual (abort|cancel)|client abort|USER_CANCEL"
                    + "|killed manually|client closed",
                    LoadErrorCode.USER_CANCELLED),
            // A connection failure that names a broker, a storage path or a message system is a
            // source failure. Before BE_UNAVAILABLE, which takes the remaining connection failures.
            rule(SOURCE_PEER + "[\\s\\S]*" + CONNECTION_FAILURE
                    + "|" + CONNECTION_FAILURE + "[\\s\\S]*(" + STORAGE_SCHEME + "|\\b(kafka|pulsar)\\b)",
                    LoadErrorCode.SOURCE_READ_FAILED),
            // Backend down, RPC failure, worker not found, replicas short of quorum. Before TIMEOUT
            // so that the brpc deadline message counts as an RPC failure.
            rule(Pattern.quote(LoadErrorUtils.BACKEND_BRPC_TIMEOUT.getKeywords())
                    + "|RpcException|rpc fail|brpc|" + CONNECTION_FAILURE
                    + "|cancell?ed by crash of backends"
                    + "|backend \\S+ is down|coordinate BE is down|not alive"
                    + "|Backend node not found|Compute node not found|No alive (backend|compute node|node)"
                    + "|No backend( or compute node)? alive|(backend|node) \\[[^\\]]*\\] is not available"
                    + "|Failed to send proxy request|Failed to find backend to execute"
                    + "|no alive replicas|less than quorum|Tablet lost replicas|few (healthy )?replicas",
                    LoadErrorCode.BE_UNAVAILABLE),
            // Column count/type mismatch, schema change during the load, missing partition. Before
            // SOURCE_READ_FAILED because these messages often quote the offending file path. A Kafka
            // topic without partitions is left to the source rule.
            rule("column count|Unknown column|column .* (not found|does not exist)|does not exist in table"
                    + "|schema change|schema (has )?changed|type mismatch"
                    + "|partition .*(not exist|does not exist|not found|no longer exists|was dropped)"
                    + "|No partition(?! in topic)",
                    LoadErrorCode.SCHEMA_MISMATCH),
            // Source side: broker, HDFS, object storage, local files, Kafka/Pulsar. A URL scheme or a
            // message system name only counts next to a failed open/read/list/connect, or next to an
            // explicit storage error code. Before TIMEOUT so Kafka metadata timeouts stay with the source.
            rule("Broker (list path|open reader|read|open writer|write|delete path)|broker .*(failed|exception)"
                    + "|fail(ed)? to connect (to )?(hdfs|namenode)"
                    + "|(open|read|list|connect|access|fetch|stat|download)[^.]{0,60}" + STORAGE_SCHEME
                    + "|NoSuchKey|NoSuchBucket|AccessDenied|Permission denied|No such file"
                    + "|file (does )?not (found|exist)|No files were found|not found or is not content"
                    + "|(fail(ed|ure)?|error|timeout|timed out|too busy|cannot|unable|invalid)[^.]{0,60}(kafka|pulsar)"
                    + "|(kafka|pulsar)[^.]{0,60}(fail|error|timeout|timed out|unreachable|unavailable|not found)"
                    + "|Offset out of range|no partition in topic|(broker|hdfs)[^.]{0,40}unreachable",
                    LoadErrorCode.SOURCE_READ_FAILED),
            // Load, transaction, publish and lock timeouts: "timeout" must sit next to what timed out.
            // "reached its timeout" is ERR_TIMEOUT, whose subject is Insert, Update, Delete, MergeInto or Query.
            rule("\\b(load(ing)?|txn|transaction|publish|commit|prepare|lock|execution|coordinator|job|query|task"
                    + "|pending|flush)\\b[^.]{0,60}\\b(timeout|timed out)\\b"
                    + "|\\b(timeout|timed out)\\b[^.]{0,40}\\b(txn manager|transaction|load|publish|after \\d+ ?ms)\\b"
                    + "|reached its timeout|deadline exceeded",
                    LoadErrorCode.TIMEOUT),
            // Publish conflicts and transactions committed or aborted by another operation.
            rule("version (already )?exist|version conflict|publish version (failed|conflict)"
                    + "|Fail(ed)? to publish version|(txn|transaction|conflicting) conflict|conflict(ing)? operation"
                    + "|already (been )?committed|already aborted|aborted by (another|other)|received other writes",
                    LoadErrorCode.TXN_CONFLICT),
            // Failures the FE classifies as internal, plus txn-log and primary-key index errors
            // raised by the BE, and loads cancelled by an FE restart or leader change.
            rule("internal error|INTERNAL_ERROR|NullPointerException|IllegalStateException"
                    + "|Cannot transition state|incomplete combined txn log|txn logs? (not|cannot be) found"
                    + "|tablet metadata( version)? not found|transaction status unknown"
                    + "|in error state|sst write|FE restart|Leader changed", LoadErrorCode.INTERNAL_ERROR)
    );

    /**
     * Maps a load job's {@link FailMsg}. The cancel type is used when it already names
     * the reason; the remaining types fall back to the message.
     */
    public static LoadErrorCode fromFailMsg(FailMsg failMsg) {
        if (failMsg == null) {
            return LoadErrorCode.UNKNOWN;
        }
        FailMsg.CancelType cancelType = failMsg.getCancelType();
        if (cancelType != null) {
            switch (cancelType) {
                case USER_CANCEL:
                    return LoadErrorCode.USER_CANCELLED;
                case ETL_QUALITY_UNSATISFIED:
                    return LoadErrorCode.FILTER_RATIO_EXCEEDED;
                case TIMEOUT:
                    return LoadErrorCode.TIMEOUT;
                case TXN_UNKNOWN:
                    return LoadErrorCode.INTERNAL_ERROR;
                default:
                    break;
            }
        }
        return fromMessage(failMsg.getMsg());
    }

    /**
     * Maps a free-text failure reason, such as a stream load task's error message or a
     * transaction abort reason, to the first matching rule; {@link LoadErrorCode#UNKNOWN}
     * when nothing matches.
     */
    public static LoadErrorCode fromMessage(String message) {
        if (Strings.isNullOrEmpty(message)) {
            return LoadErrorCode.UNKNOWN;
        }
        for (Rule rule : RULES) {
            if (rule.pattern().matcher(message).find()) {
                return rule.code();
            }
        }
        return LoadErrorCode.UNKNOWN;
    }
}
