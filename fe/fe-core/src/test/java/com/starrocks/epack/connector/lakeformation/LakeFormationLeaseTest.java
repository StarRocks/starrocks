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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TUniqueId;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The last gate before credentials reach the wire.
 *
 * Every refusal here corresponds to a way a plan can be reused that the planner cannot prevent - a retry
 * that did not re-plan, a prepared statement executed again, a plan cached by the MV rewriter. The point of
 * testing them one at a time is that each is a separate hole: passing five of six checks is not "mostly
 * safe", it is a leak through the sixth.
 */
public class LakeFormationLeaseTest {

    /** Any non-zero value: what the lease does with it is carry it, which is what these tests need. */
    private static final long CACHE_SCOPE_SEED = 0x5eed5eed5eed5eedL;

    private static final String ACCESS_KEY = "AKIAEXAMPLETESTKEY";
    private static final String SECRET_KEY = "wJalrXUtnFEMIsecretKEYtestVALUE";
    private static final String SESSION_TOKEN = "FwoGZXIvYXdzTESTsessionTOKENvalue";

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf", null, "us-west-2", "db", "t");
    private static final LakeFormationTableIdentity OTHER_TABLE =
            new LakeFormationTableIdentity("lf", null, "us-west-2", "db", "other");

    private static final UserIdentity ALICE = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");
    private static final UserIdentity BOB = UserIdentity.createAnalyzedUserIdentWithIp("bob", "%");

    private ConnectContext context;

    @Mocked
    private GlueClient glueClient;
    @Mocked
    private LakeFormationClient lakeFormationClient;

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static LakeFormationCatalogProperties properties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("hive.metastore.type", "glue");
        raw.put("catalog.access.control", "lakeformation");
        raw.put("aws.lakeformation.session_tag_value", "starrocks");
        raw.put("aws.glue.region", "us-west-2");
        return LakeFormationCatalogProperties.from("hive", raw);
    }

    private static LakeFormationTableAccess access(LakeFormationTableIdentity identity, Instant expiry) {
        return LakeFormationTableAccess.from(identity, "auth-id",
                GetTemporaryGlueTableCredentialsResponse.builder()
                        .accessKeyId(ACCESS_KEY)
                        .secretAccessKey(SECRET_KEY)
                        .sessionToken(SESSION_TOKEN)
                        .expiration(expiry)
                        .build());
    }

    private ConnectContext contextFor(UserIdentity user, UUID queryId, TUniqueId executionId) {
        ConnectContext ctx = new ConnectContext();
        ctx.setQueryId(queryId);
        ctx.setExecutionId(executionId);
        ctx.setCurrentUserIdentity(user);
        ctx.setThreadLocalInfo();
        context = ctx;
        return ctx;
    }

    /**
     * A lease that can actually be renewed, so admission takes the refresh path rather than running with
     * what it has. Counts the vends, because how many times Lake Formation is asked is the property under
     * test - not what it answers.
     *
     * @param renewedFor the identity the renewed credentials claim to be for; pass another table to
     *                   exercise a re-vend that comes back wrong
     * @param vendFailure thrown instead of answering
     */
    private LakeFormationLease renewableLease(Instant expiry, AtomicInteger vendCalls,
                                              LakeFormationTableIdentity renewedFor,
                                              RuntimeException vendFailure) {
        new MockUp<LakeFormationMetadataGateway>() {
            @Mock
            public LakeFormationTableAccess vendTableCredentials(LakeFormationTableIdentity identity,
                                                                 String tableArn,
                                                                 String queryAuthorizationId,
                                                                 LakeFormationQuerySession session) {
                vendCalls.incrementAndGet();
                if (vendFailure != null) {
                    throw vendFailure;
                }
                return access(renewedFor, Instant.now().plus(1, ChronoUnit.HOURS));
            }
        };
        UUID queryId = UUID.randomUUID();
        TUniqueId executionId = new TUniqueId(1, 1);
        contextFor(ALICE, queryId, executionId);
        LakeFormationLeaseContext leaseContext = LakeFormationLeaseContext.create(IDENTITY, ALICE,
                "attempt-1", queryId.toString(), executionId,
                new LakeFormationMetadataGateway(glueClient, lakeFormationClient, properties()),
                new LakeFormationQuerySession(queryId.toString(), Instant.now(), "lf-e2e"),
                "arn:aws:glue:us-west-2:1:table/db/t", "s3://bucket/db/t", properties(), Map.of(), CACHE_SCOPE_SEED);
        LakeFormationLease lease = LakeFormationLease.validated(leaseContext, access(IDENTITY, expiry));
        leaseContext.holder().set(lease);
        return lease;
    }

    private static LakeFormationLease leaseFor(LakeFormationTableIdentity identity, UserIdentity principal,
                                               String attemptId, UUID queryId, TUniqueId executionId,
                                               Instant expiry) {
        LakeFormationLeaseContext leaseContext = LakeFormationLeaseContext.create(identity, principal,
                attemptId, queryId.toString(), executionId, null, null, null, "s3://bucket/db/t",
                properties(), Map.of(), CACHE_SCOPE_SEED);
        LakeFormationLease lease = LakeFormationLease.validated(leaseContext, access(identity, expiry));
        leaseContext.holder().set(lease);
        return lease;
    }

    /** The shape everything else deviates from: right table, right user, right attempt, plenty of time. */
    private LakeFormationLease usableLease() {
        UUID queryId = UUID.randomUUID();
        TUniqueId executionId = new TUniqueId(1, 1);
        contextFor(ALICE, queryId, executionId);
        return leaseFor(IDENTITY, ALICE, "attempt-1", queryId, executionId,
                Instant.now().plus(1, ChronoUnit.HOURS));
    }

    @Test
    public void testUsableLeaseYieldsAwsCredentials() {
        LakeFormationLease lease = usableLease();
        CloudConfiguration configuration = lease.cloudConfiguration(context);

        assertNotNull(configuration);
        assertEquals(CloudType.AWS, configuration.getCloudType());
    }

    @Test
    public void testSerializingOutsideAQueryIsRefused() {
        LakeFormationLease lease = usableLease();
        assertRefused(() -> lease.cloudConfiguration(null), "outside a query context");
    }

    @Test
    public void testCredentialsFromAnotherQueryAreRefused() {
        LakeFormationLease lease = usableLease();
        ConnectContext otherQuery = contextFor(ALICE, UUID.randomUUID(), new TUniqueId(1, 1));

        assertRefused(() -> lease.cloudConfiguration(otherQuery), "belong to query");
    }

    /** A retry replaces the execution id; a plan that was not rebuilt still holds the previous one. */
    @Test
    public void testCredentialsFromAnEarlierExecutionAttemptAreRefused() {
        UUID queryId = UUID.randomUUID();
        contextFor(ALICE, queryId, new TUniqueId(1, 1));
        LakeFormationLease lease = leaseFor(IDENTITY, ALICE, "attempt-1", queryId, new TUniqueId(1, 1),
                Instant.now().plus(1, ChronoUnit.HOURS));

        ConnectContext retry = contextFor(ALICE, queryId, new TUniqueId(2, 2));
        assertRefused(() -> lease.cloudConfiguration(retry), "earlier execution attempt");
    }

    @Test
    public void testCredentialsIssuedForAnotherUserAreRefused() {
        LakeFormationLease lease = usableLease();
        ConnectContext asBob = contextFor(BOB, UUID.fromString(context.getQueryId().toString()),
                context.getExecutionId());

        assertRefused(() -> lease.cloudConfiguration(asBob), "user running this query");
    }

    /**
     * A path that lost the user is a path that cannot show these credentials belong to whoever is running
     * the query, so it is refused rather than waved through. "Missing" is not "matching".
     */
    @Test
    public void testAMissingUserIsRefusedRatherThanTreatedAsAMatch() {
        UUID queryId = UUID.randomUUID();
        TUniqueId executionId = new TUniqueId(1, 1);
        contextFor(ALICE, queryId, executionId);
        LakeFormationLease lease = leaseFor(IDENTITY, null, "attempt-1", queryId, executionId,
                Instant.now().plus(1, ChronoUnit.HOURS));

        assertRefused(() -> lease.cloudConfiguration(context), "user running this query");
    }

    /**
     * Refused while still nominally valid. The clock that stamped the expiry is not FE's, and a credential
     * that dies in transit reaches the BE as a permissions error rather than as the timing problem it is.
     */
    @Test
    public void testCredentialsAboutToExpireAreRefusedNotJustExpiredOnes() {
        UUID queryId = UUID.randomUUID();
        TUniqueId executionId = new TUniqueId(1, 1);
        contextFor(ALICE, queryId, executionId);
        LakeFormationLease lease = leaseFor(IDENTITY, ALICE, "attempt-1", queryId, executionId,
                Instant.now().plus(5, ChronoUnit.SECONDS));

        assertRefused(() -> lease.cloudConfiguration(context), "too soon to start a scan");
    }

    /** A credential that does not cover the table it was requested for cannot read it. */
    @Test
    public void testCredentialsNotCoveringTheTableRootAreRefusedAtConstruction() {
        LakeFormationLeaseContext leaseContext = LakeFormationLeaseContext.create(IDENTITY, ALICE,
                "attempt-1", UUID.randomUUID().toString(), new TUniqueId(1, 1), null, null, null,
                "s3://bucket/db/t", properties(), Map.of(), CACHE_SCOPE_SEED);
        LakeFormationTableAccess elsewhere = LakeFormationTableAccess.from(IDENTITY, "auth-id",
                GetTemporaryGlueTableCredentialsResponse.builder()
                        .accessKeyId(ACCESS_KEY).secretAccessKey(SECRET_KEY).sessionToken(SESSION_TOKEN)
                        .expiration(Instant.now().plus(1, ChronoUnit.HOURS))
                        .vendedS3Path("s3://bucket/db/somewhere-else")
                        .build());

        assertRefused(() -> LakeFormationLease.validated(leaseContext, elsewhere),
                "do not cover the table's own location");
    }

    /** A root on another scheme is refused even when the response names no vended paths to compare with. */
    @Test
    public void testANonS3TableRootIsRefusedWithoutVendedPaths() {
        LakeFormationLeaseContext leaseContext = LakeFormationLeaseContext.create(IDENTITY, ALICE,
                "attempt-1", UUID.randomUUID().toString(), new TUniqueId(1, 1), null, null, null,
                "hdfs://namenode/db/t", properties(), Map.of(), CACHE_SCOPE_SEED);

        assertThrows(LakeFormationTableAccessException.class, () -> LakeFormationLease.validated(leaseContext,
                access(IDENTITY, Instant.now().plus(1, ChronoUnit.HOURS))));
    }

    @Test
    public void testCredentialsForAnotherTableAreRefusedAtConstruction() {
        LakeFormationLeaseContext leaseContext = LakeFormationLeaseContext.create(IDENTITY, ALICE,
                "attempt-1", UUID.randomUUID().toString(), new TUniqueId(1, 1), null, null, null,
                "s3://bucket/db/t", properties(), Map.of(), CACHE_SCOPE_SEED);

        assertRefused(() -> LakeFormationLease.validated(leaseContext,
                        access(OTHER_TABLE, Instant.now().plus(1, ChronoUnit.HOURS))),
                "while lf.db.t");
    }

    /**
     * This object is one of the few holding plaintext credentials, so it has to stay unprintable even when
     * something logs it by accident - and every refusal message it produces has to stay quotable.
     */
    @Test
    public void testNeitherToStringNorAnyRefusalRevealsACredential() {
        LakeFormationLease lease = usableLease();
        assertNoSecret(lease.toString());

        ConnectContext otherQuery = contextFor(BOB, UUID.randomUUID(), new TUniqueId(9, 9));
        LakeFormationTableAccessException refusal = assertThrows(LakeFormationTableAccessException.class,
                () -> lease.cloudConfiguration(otherQuery));
        assertNoSecret(refusal.getMessage());
    }

    /** Admission grants the whole requested window when the credentials already outlast it. */
    @Test
    public void testALeaseThatAlreadyCoversTheDeadlineGrantsTheWholeWindow() {
        LakeFormationLease lease = usableLease();
        Instant requested = Instant.now().plus(5, ChronoUnit.MINUTES);

        assertEquals(requested, lease.admitForExecution(requested).effectiveDeadline());
        assertTrue(lease.matches(context));
    }

    /**
     * The behaviour Task 11 exists for. A statement timeout says how long a query is *allowed* to run, not
     * how long it will: with insert_timeout defaulting to four hours and Lake Formation capping a lease at
     * one, demanding full coverage refused every INSERT ... SELECT, a single row as surely as a terabyte.
     *
     * So a credential that cannot reach the requested deadline shortens the query instead of refusing it.
     * This lease has no gateway, so it cannot even be renewed - and what it has left is still a real window.
     */
    @Test
    public void testAShortLeaseShortensTheQueryRatherThanRefusingIt() {
        UUID queryId = UUID.randomUUID();
        contextFor(ALICE, queryId, new TUniqueId(1, 1));
        Instant expiry = Instant.now().plus(30, ChronoUnit.MINUTES);
        LakeFormationLease lease = leaseFor(IDENTITY, ALICE, "attempt-1", queryId, new TUniqueId(1, 1),
                expiry);

        Instant requested = Instant.now().plus(4, ChronoUnit.HOURS);
        Instant granted = lease.admitForExecution(requested).effectiveDeadline();

        // The credential's own safe end, not the four hours that were asked for.
        assertEquals(expiry.minus(LakeFormationLease.CLOCK_SKEW_GUARD), granted);
        assertTrue(granted.isBefore(requested));
    }

    /**
     * The one case that still refuses: no window at all. Inside the clock-skew guard there is no moment
     * left that the credential can safely be used for, so the query provably cannot do any work.
     */
    @Test
    public void testAQueryIsRefusedOnlyWhenNoSafeWindowRemains() {
        UUID queryId = UUID.randomUUID();
        contextFor(ALICE, queryId, new TUniqueId(1, 1));
        LakeFormationLease lease = leaseFor(IDENTITY, ALICE, "attempt-1", queryId, new TUniqueId(1, 1),
                Instant.now().plus(10, ChronoUnit.SECONDS));

        LakeFormationTableAccessException refusal = assertThrows(LakeFormationTableAccessException.class,
                () -> lease.admitForExecution(Instant.now().plus(1, ChronoUnit.HOURS)));

        assertTrue(refusal.getMessage().contains("no safe execution"), refusal.getMessage());
        // The message must not send the user off to change a timeout, which cannot help here.
        assertTrue(refusal.getMessage().contains("will not help"), refusal.getMessage());
        assertNoSecret(refusal.getMessage());
    }

    /**
     * A self-join admits the same table once per scan node. Each of them landing its own re-vend would
     * multiply the credential requests for one statement, and - worse - let the two halves of the join end
     * up holding credentials from different renewal windows.
     */
    @Test
    public void testEveryScanOnOneTableSharesASingleAdmissionRefresh() {
        AtomicInteger vendCalls = new AtomicInteger();
        LakeFormationLease lease = renewableLease(
                Instant.now().plus(20, ChronoUnit.MINUTES), vendCalls, IDENTITY, null);
        Instant requested = Instant.now().plus(4, ChronoUnit.HOURS);

        Instant first = lease.admitForExecution(requested).effectiveDeadline();
        Instant second = lease.admitForExecution(requested).effectiveDeadline();

        assertEquals(1, vendCalls.get(), "one refresh per table, however many scans ask");
        assertEquals(first, second, "both scans must be held to the same window");
    }

    /**
     * A refresh that fails is replayed, not retried: the next scan node on this table gets the same
     * refusal. Retrying would ask Lake Formation twice inside one statement and could get two different
     * answers, with the second winning for no better reason than arriving later.
     *
     * The lease also stays refused rather than quietly falling back to what it had. Whatever is left on the
     * old credentials is not the point - the point is that a re-vend that failed is not evidence of
     * anything, and nothing here may reach for the catalog's own credentials instead.
     */
    @Test
    public void testAFailedRefreshIsReplayedAndNeverFallsBackToTheOldLease() {
        AtomicInteger vendCalls = new AtomicInteger();
        LakeFormationLease lease = renewableLease(
                Instant.now().plus(20, ChronoUnit.MINUTES), vendCalls, IDENTITY,
                new LakeFormationTableAccessException("Lake Formation refused to re-vend"));
        Instant requested = Instant.now().plus(4, ChronoUnit.HOURS);

        LakeFormationTableAccessException first = assertThrows(LakeFormationTableAccessException.class,
                () -> lease.admitForExecution(requested));
        LakeFormationTableAccessException second = assertThrows(LakeFormationTableAccessException.class,
                () -> lease.admitForExecution(requested));

        assertEquals(1, vendCalls.get(), "a failed refresh must not be retried by the next scan");
        assertEquals(first.getMessage(), second.getMessage(), "the same answer, replayed");
        assertNoSecret(first.getMessage());
    }

    /**
     * The renewed credentials go through the same identity check as the first ones. A re-vend that comes
     * back for another table is a failed refresh, not a usable lease - otherwise admission would be the one
     * place where credentials enter without being checked against the table they are meant for.
     */
    @Test
    public void testARefreshThatComesBackForAnotherTableIsRefused() {
        AtomicInteger vendCalls = new AtomicInteger();
        LakeFormationLease lease = renewableLease(
                Instant.now().plus(20, ChronoUnit.MINUTES), vendCalls, OTHER_TABLE, null);

        LakeFormationTableAccessException refusal = assertThrows(LakeFormationTableAccessException.class,
                () -> lease.admitForExecution(Instant.now().plus(4, ChronoUnit.HOURS)));

        assertEquals(1, vendCalls.get());
        // The identity check is what has to reject this, not the window check: the renewed credentials are
        // good for an hour, so a lease that accepted them would have had plenty of room and never refused.
        assertTrue(refusal.getMessage().contains("while lf.db.t"), refusal.getMessage());
        assertNoSecret(refusal.getMessage());
    }

    @Test
    public void testMatchesIsFalseForAnotherQueryRatherThanThrowing() {
        LakeFormationLease lease = usableLease();
        assertFalse(lease.matches(contextFor(ALICE, UUID.randomUUID(), new TUniqueId(1, 1))));
        assertFalse(lease.matches(null));
    }

    private static void assertRefused(Runnable call, String expectedFragment) {
        LakeFormationTableAccessException e =
                assertThrows(LakeFormationTableAccessException.class, call::run);
        assertTrue(e.getMessage().contains(expectedFragment),
                "expected the refusal to say why; got: " + e.getMessage());
        assertNoSecret(e.getMessage());
    }

    private static void assertNoSecret(String text) {
        assertFalse(text.contains(ACCESS_KEY), "an access key leaked into: " + text);
        assertFalse(text.contains(SECRET_KEY), "a secret key leaked into: " + text);
        assertFalse(text.contains(SESSION_TOKEN), "a session token leaked into: " + text);
    }
}
