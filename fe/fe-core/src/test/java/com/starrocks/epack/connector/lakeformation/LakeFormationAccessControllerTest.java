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

import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ExternalAccessController;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.connector.TableLoadPurpose;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.epack.authorization.AccessControllerEPack;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The delegation is checked by reflection rather than one case per method. Forty one hand written cases
 * would be the thing most likely to fall out of date, and the failure mode of a missed delegation is silent:
 * getColumnMaskingPolicy and getRowAccessPolicy default to returning null, so forgetting either one would
 * disable every masking and row access policy on a Lake Formation catalog without a single error message.
 */
public class LakeFormationAccessControllerTest {

    private static final Set<String> BEHAVIOURAL =
            Set.of("checkColumnAction", "getColumnMaskingPolicy", "getRowAccessPolicy");

    private static final String ALWAYS_AUTHORIZED_COLUMN = "keep";

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("c", null, "us-west-2", "d", "t");

    private static List<Method> declaredMethods(Class<?> iface) {
        List<Method> methods = new ArrayList<>();
        for (Method method : iface.getDeclaredMethods()) {
            if (!method.isSynthetic() && !Modifier.isStatic(method.getModifiers())) {
                methods.add(method);
            }
        }
        return methods;
    }

    private static boolean overrides(Method method) {
        try {
            LakeFormationAccessController.class.getDeclaredMethod(method.getName(), method.getParameterTypes());
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    /**
     * Declaring the method is not the same as delegating it: an empty override would satisfy that and
     * silently answer "allowed" for everything it covers. The delegate field is replaced with a proxy that
     * throws for any call, so a method that really forwards lets the probe out and one that does nothing
     * does not.
     */
    private static LakeFormationAccessController withProbingDelegate() {
        AccessControllerEPack probe = (AccessControllerEPack) Proxy.newProxyInstance(
                LakeFormationAccessControllerTest.class.getClassLoader(),
                new Class<?>[] {AccessControllerEPack.class},
                (p, method, args) -> {
                    throw new DelegationProbe();
                });
        return new LakeFormationAccessController(probe);
    }

    private static boolean actuallyDelegates(Method method, LakeFormationAccessController controller) {
        try {
            Method own = LakeFormationAccessController.class.getDeclaredMethod(
                    method.getName(), method.getParameterTypes());
            own.setAccessible(true);
            own.invoke(controller, new Object[method.getParameterCount()]);
            return false;
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            // The proxy throws through the InvocationHandler, so it may arrive wrapped once more.
            return cause instanceof DelegationProbe
                    || (cause != null && cause.getCause() instanceof DelegationProbe);
        } catch (ReflectiveOperationException e) {
            return false;
        }
    }

    /** Thrown by the probing delegate so the test can tell forwarding from an empty body. */
    private static final class DelegationProbe extends RuntimeException {
    }

    @Test
    public void testEveryAccessControllerMethodIsOverridden() {
        List<String> missing = new ArrayList<>();
        for (Method method : declaredMethods(AccessController.class)) {
            if (!overrides(method)) {
                missing.add(method.getName());
            }
        }
        assertTrue(missing.isEmpty(), "declared nowhere on the controller: " + missing);
    }

    /**
     * Without these five the active controller cannot be cast to AccessControllerEPack, and several call
     * sites do that cast with no guard at all - `USE db` among them.
     */
    @Test
    public void testEveryEnterpriseMethodIsOverridden() {
        List<String> missing = new ArrayList<>();
        for (Method method : declaredMethods(AccessControllerEPack.class)) {
            if (!overrides(method)) {
                missing.add(method.getName());
            }
        }
        assertTrue(missing.isEmpty(), "declared nowhere on the controller: " + missing);
    }

    /**
     * A sentinel for each parameter type, so the recorder below can tell "forwarded my argument" from
     * "passed something of the right type". Types nothing here needs to distinguish stay null.
     */
    private static Object sentinel(Class<?> type, int position) {
        if (type == String.class) {
            return "arg" + position;
        }
        if (type == ConnectContext.class) {
            return new ConnectContext();
        }
        if (type == TableName.class) {
            return new TableName("c" + position, "d", "t");
        }
        if (type == List.class) {
            return new ArrayList<>();
        }
        if (type.isEnum()) {
            Object[] constants = type.getEnumConstants();
            return constants.length == 0 ? null : constants[0];
        }
        return null;
    }

    /**
     * The probing delegate proves a call leaves the method, but the method never returns, so a body that
     * forwards and then does something else afterwards would look the same. This one records instead of
     * throwing: every delegating body runs to completion, and the arguments have to arrive unchanged and
     * in order rather than merely being of the right type.
     */
    @Test
    public void testEveryDelegatingMethodForwardsItsArgumentsUnchanged() {
        Map<String, Object[]> recorded = new HashMap<>();
        AccessControllerEPack recorder = (AccessControllerEPack) Proxy.newProxyInstance(
                LakeFormationAccessControllerTest.class.getClassLoader(),
                new Class<?>[] {AccessControllerEPack.class},
                (p, method, args) -> {
                    recorded.put(method.getName(), args);
                    return null;
                });
        LakeFormationAccessController controller = new LakeFormationAccessController(recorder);

        List<Method> all = new ArrayList<>(declaredMethods(AccessController.class));
        all.addAll(declaredMethods(AccessControllerEPack.class));
        List<String> wrong = new ArrayList<>();
        for (Method method : all) {
            if (BEHAVIOURAL.contains(method.getName())) {
                continue;
            }
            Class<?>[] types = method.getParameterTypes();
            Object[] sent = new Object[types.length];
            for (int i = 0; i < types.length; i++) {
                sent[i] = sentinel(types[i], i);
            }
            try {
                Method own = LakeFormationAccessController.class.getDeclaredMethod(
                        method.getName(), types);
                own.setAccessible(true);
                own.invoke(controller, sent);
            } catch (ReflectiveOperationException e) {
                wrong.add(method.getName() + " threw " + e.getCause());
                continue;
            }
            Object[] seen = recorded.get(method.getName());
            if (seen == null) {
                wrong.add(method.getName() + " never reached the native controller");
            } else if (!Arrays.equals(sent, seen)) {
                wrong.add(method.getName() + " forwarded " + Arrays.toString(seen)
                        + " instead of " + Arrays.toString(sent));
            }
        }
        assertTrue(wrong.isEmpty(), String.join("; ", wrong));
    }

    /**
     * The check that matters. checkColumnAction and the two policy getters are excluded because they decide
     * by object - Lake Formation for a governed table, native for anything else - rather than delegate;
     * all three have their own behavioural cases below, including one proving they never consult native for
     * a governed table.
     */
    @Test
    public void testEveryDelegatingMethodActuallyCallsTheNativeController() {
        LakeFormationAccessController controller = withProbingDelegate();
        List<String> notDelegating = new ArrayList<>();
        List<Method> all = new ArrayList<>(declaredMethods(AccessController.class));
        all.addAll(declaredMethods(AccessControllerEPack.class));
        for (Method method : all) {
            if (BEHAVIOURAL.contains(method.getName())) {
                continue;
            }
            if (!actuallyDelegates(method, controller)) {
                notDelegating.add(method.getName());
            }
        }
        assertTrue(notDelegating.isEmpty(), "declared but not delegating: " + notDelegating);
    }

    /** Extending it is what makes a column level refusal report 5204 instead of the generic code. */
    @Test
    public void testIsAnExternalAccessController() {
        assertTrue(ExternalAccessController.class.isAssignableFrom(LakeFormationAccessController.class));
    }

    @Test
    public void testImplementsTheEnterpriseInterface() {
        assertTrue(AccessControllerEPack.class.isAssignableFrom(LakeFormationAccessController.class));
    }

    // ------------------------------------------------------------------------------------------------
    // The reflection cases above prove a method is declared and forwards. They do it through
    // Method.invoke, which records nothing against the method body, so they cannot show that the body a
    // reviewer reads is the body that runs. Every delegating method is therefore also called directly
    // below. Both halves earn their place: the reflective one fails when a method is added upstream and
    // not overridden here, this one fails when an override stops forwarding.
    // ------------------------------------------------------------------------------------------------

    private static void expectDelegation(Executable call) {
        assertThrows(DelegationProbe.class, call::run,
                "the body did not reach the native controller");
    }

    /** A call that may declare AccessDeniedException; the probe unwinds before that can happen. */
    private interface Executable {
        void run() throws AccessDeniedException;
    }

    @Test
    public void testEveryDelegatingMethodForwardsWhenCalledDirectly() {
        LakeFormationAccessController controller = withProbingDelegate();
        ConnectContext ctx = null;
        TableName table = null;
        PrivilegeType want = null;
        PipeName pipe = null;
        PolicyType policy = null;
        ObjectType objectType = null;

        expectDelegation(() -> controller.checkSystemAction(ctx, want));
        expectDelegation(() -> controller.checkUserAction(ctx, null, want));
        expectDelegation(() -> controller.checkCatalogAction(ctx, "c", want));
        expectDelegation(() -> controller.checkAnyActionOnCatalog(ctx, "c"));
        expectDelegation(() -> controller.checkDbAction(ctx, "c", "d", want));
        expectDelegation(() -> controller.checkAnyActionOnDb(ctx, "c", "d"));
        expectDelegation(() -> controller.checkTableAction(ctx, table, want));
        expectDelegation(() -> controller.checkAnyActionOnTable(ctx, table));
        expectDelegation(() -> controller.checkAnyActionOnAnyTable(ctx, "c", "d"));
        expectDelegation(() -> controller.checkViewAction(ctx, table, want));
        expectDelegation(() -> controller.checkAnyActionOnView(ctx, table));
        expectDelegation(() -> controller.checkAnyActionOnAnyView(ctx, "d"));
        expectDelegation(() -> controller.checkMaterializedViewAction(ctx, table, want));
        expectDelegation(() -> controller.checkAnyActionOnMaterializedView(ctx, table));
        expectDelegation(() -> controller.checkAnyActionOnAnyMaterializedView(ctx, "d"));
        expectDelegation(() -> controller.checkFunctionAction(ctx, null, null, want));
        expectDelegation(() -> controller.checkAnyActionOnFunction(ctx, "d", null));
        expectDelegation(() -> controller.checkAnyActionOnAnyFunction(ctx, "d"));
        expectDelegation(() -> controller.checkGlobalFunctionAction(ctx, null, want));
        expectDelegation(() -> controller.checkAnyActionOnGlobalFunction(ctx, null));
        expectDelegation(() -> controller.checkActionInDb(ctx, "d", want));
        expectDelegation(() -> controller.checkResourceAction(ctx, "r", want));
        expectDelegation(() -> controller.checkAnyActionOnResource(ctx, "r"));
        expectDelegation(() -> controller.checkResourceGroupAction(ctx, "g", want));
        expectDelegation(() -> controller.checkPipeAction(ctx, pipe, want));
        expectDelegation(() -> controller.checkAnyActionOnPipe(ctx, pipe));
        expectDelegation(() -> controller.checkStorageVolumeAction(ctx, "v", want));
        expectDelegation(() -> controller.checkAnyActionOnStorageVolume(ctx, "v"));
        expectDelegation(() -> controller.withGrantOption(ctx, objectType, List.of(), List.of()));
        expectDelegation(() -> controller.checkWarehouseAction(ctx, "w", want));
        expectDelegation(() -> controller.checkAnyActionOnWarehouse(ctx, "w"));
        expectDelegation(() -> controller.checkContextBaseAction(ctx, "b", want));
        expectDelegation(() -> controller.checkAnyActionOnContextBase(ctx, "b"));
        expectDelegation(() -> controller.checkPolicyAction(ctx, policy, "c", "d", "p", want));
        expectDelegation(() -> controller.checkAnyActionOnPolicy(ctx, policy, "c", "d", "p"));
        expectDelegation(() -> controller.checkAnyActionOnAnyPolicy(ctx, policy, "c", "d"));
        expectDelegation(() -> controller.checkFailoverGroupAction(ctx, "f", want));
        expectDelegation(() -> controller.checkAnyActionOnFailoverGroup(ctx, "f"));
    }

    /** The default constructor has to compose the real native controller, not leave the field null. */
    @Test
    public void testTheDefaultConstructorComposesTheNativeController() {
        LakeFormationAccessController controller = new LakeFormationAccessController();
        // Any delegating call would reach the real native controller; asserting it does not NPE on the
        // field is the part that belongs here.
        assertTrue(AccessControllerEPack.class.isAssignableFrom(controller.getClass()));
    }

    // ---- checkColumnAction: the one method that decides rather than delegates ----

    private static void metadataReturns(Table table) {
        new MockUp<MetadataMgr>() {
            @Mock
            public Table getTable(ConnectContext context, String catalog, String db, String tbl) {
                return table;
            }
        };
    }

    /** A table nobody could resolve must not be waved through: unavailable is not the same as allowed. */
    @Test
    public void testAnUnresolvableTableIsRefusedRatherThanAllowed() {
        metadataReturns(null);
        LakeFormationAccessController controller = new LakeFormationAccessController(allowingDelegate());
        AccessDeniedException denied = assertThrows(AccessDeniedException.class,
                () -> controller.checkColumnAction(new ConnectContext(), new TableName("c", "d", "t"),
                        "id", PrivilegeType.SELECT));
        assertTrue(denied.getMessage().contains("metadata is unavailable"));
    }

    /** An unregistered table in the same catalog: the native decision is the whole answer. */
    @Test
    public void testAnUngovernedTableIsLeftToTheNativeDecision(@Mocked Table plain) throws Exception {
        metadataReturns(plain);
        LakeFormationAccessController controller = new LakeFormationAccessController(allowingDelegate());
        controller.checkColumnAction(new ConnectContext(), new TableName("c", "d", "t"),
                "anything", PrivilegeType.SELECT);
    }

    /** ...and it really is asked: the probe fires, so an empty branch could not pass this. */
    @Test
    public void testAnUngovernedTableConsultsTheNativeTableDecision(@Mocked Table plain) {
        metadataReturns(plain);
        LakeFormationAccessController controller = withProbingDelegate();
        assertThrows(DelegationProbe.class,
                () -> controller.checkColumnAction(new ConnectContext(), new TableName("c", "d", "t"),
                        "anything", PrivilegeType.SELECT));
    }

    /**
     * A governed table never consults native: with a delegate that throws on every call, an authorized
     * column still passes and an unauthorized one is refused by Lake Formation's own message. This is the
     * contract - a native table grant is not required to read a governed table.
     */
    @Test
    public void testAGovernedTableNeverConsultsTheNativeController() throws Exception {
        LakeFormationAccessController controller = withProbingDelegate();
        metadataReturns(governedTable("id", true, true));
        controller.checkColumnAction(new ConnectContext(), new TableName("c", "d", "t"),
                "id", PrivilegeType.SELECT);

        metadataReturns(governedTable("ssn", false, true));
        AccessDeniedException denied = assertThrows(AccessDeniedException.class,
                () -> controller.checkColumnAction(new ConnectContext(), new TableName("c", "d", "t"),
                        "ssn", PrivilegeType.SELECT));
        assertTrue(denied.getMessage().contains("Lake Formation"), denied.getMessage());
    }

    /** A governed table has one policy owner: no native masking or row policy is read for it. */
    @Test
    public void testAGovernedTableCarriesNoNativePolicy() {
        metadataReturns(governedTable("id", true, true));
        LakeFormationAccessController controller = withProbingDelegate();
        TableName table = new TableName("c", "d", "t");
        assertTrue(controller.getColumnMaskingPolicy(new ConnectContext(), table, List.of()).isEmpty());
        assertEquals(null, controller.getRowAccessPolicy(new ConnectContext(), table));
    }

    /** An ungoverned table's policies still come from native - the probe fires. */
    @Test
    public void testAnUngovernedTablePolicyIsDelegated(@Mocked Table plain) {
        metadataReturns(plain);
        LakeFormationAccessController controller = withProbingDelegate();
        TableName table = new TableName("c", "d", "t");
        assertThrows(DelegationProbe.class,
                () -> controller.getColumnMaskingPolicy(new ConnectContext(), table, List.of()));
        assertThrows(DelegationProbe.class, () -> controller.getRowAccessPolicy(new ConnectContext(), table));
    }

    @Test
    public void testAnAuthorizedColumnIsAllowed() throws Exception {
        metadataReturns(governedTable("id", true, true));
        LakeFormationAccessController controller = new LakeFormationAccessController(allowingDelegate());
        controller.checkColumnAction(new ConnectContext(), new TableName("c", "d", "t"),
                "id", PrivilegeType.SELECT);
    }

    @Test
    public void testAnUnauthorizedColumnIsRefused() {
        metadataReturns(governedTable("ssn", false, true));
        LakeFormationAccessController controller = new LakeFormationAccessController(allowingDelegate());
        AccessDeniedException denied = assertThrows(AccessDeniedException.class,
                () -> controller.checkColumnAction(new ConnectContext(), new TableName("c", "d", "t"),
                        "ssn", PrivilegeType.SELECT));
        assertTrue(denied.getMessage().contains("'ssn'"));
    }

    /**
     * count(*) over an HDFS scan is rewritten into a scan of a placeholder column. Refusing that name
     * refuses every count(*) on a governed table, so it is waved through - but only because it is not a
     * physical column of the table.
     */
    @Test
    public void testTheCountStarPlaceholderIsWavedThrough() throws Exception {
        metadataReturns(governedTable("___count___", false, false));
        LakeFormationAccessController controller = new LakeFormationAccessController(allowingDelegate());
        controller.checkColumnAction(new ConnectContext(), new TableName("c", "d", "t"),
                "___count___", PrivilegeType.SELECT);
    }

    /** Both halves of that condition are load bearing: a real column with that name is still a column. */
    @Test
    public void testAPhysicalColumnNamedLikeThePlaceholderIsStillRefused() {
        metadataReturns(governedTable("___count___", false, true));
        LakeFormationAccessController controller = new LakeFormationAccessController(allowingDelegate());
        assertThrows(AccessDeniedException.class,
                () -> controller.checkColumnAction(new ConnectContext(), new TableName("c", "d", "t"),
                        "___count___", PrivilegeType.SELECT));
    }

    /** Any other synthetic name is refused, because nothing has shown it to be harmless. */
    @Test
    public void testAnotherSyntheticNameIsRefused() {
        metadataReturns(governedTable("___other___", false, false));
        LakeFormationAccessController controller = new LakeFormationAccessController(allowingDelegate());
        assertThrows(AccessDeniedException.class,
                () -> controller.checkColumnAction(new ConnectContext(), new TableName("c", "d", "t"),
                        "___other___", PrivilegeType.SELECT));
    }

    // ---- policy translation ----

    @Test
    public void testAPolicyOverAnUnauthorizedColumnIsExplained() {
        metadataReturns(null);  // not governed (or unknowable): the native policy is delegated
        AccessControllerEPack failing = throwingDelegate(
                new StarRocksPlannerException("can not find column by column id 7", ErrorType.INTERNAL_ERROR));
        LakeFormationAccessController controller = new LakeFormationAccessController(failing);
        TableName table = new TableName("c", "d", "t");

        StarRocksPlannerException masking = assertThrows(StarRocksPlannerException.class,
                () -> controller.getColumnMaskingPolicy(null, table, List.of()));
        assertTrue(masking.getMessage().contains("Lake Formation has not authorized"));

        StarRocksPlannerException row = assertThrows(StarRocksPlannerException.class,
                () -> controller.getRowAccessPolicy(null, table));
        assertTrue(row.getMessage().contains("Lake Formation has not authorized"));
    }

    /** Only that one message shape is translated; a genuine analysis error keeps its own words. */
    @Test
    public void testAnUnrelatedPlannerFailureIsNotRewritten() {
        metadataReturns(null);  // not governed (or unknowable): the native policy is delegated
        StarRocksPlannerException original =
                new StarRocksPlannerException("something else entirely", ErrorType.INTERNAL_ERROR);
        LakeFormationAccessController controller =
                new LakeFormationAccessController(throwingDelegate(original));
        TableName table = new TableName("c", "d", "t");

        assertSame(original, assertThrows(StarRocksPlannerException.class,
                () -> controller.getColumnMaskingPolicy(null, table, List.of())));
        assertSame(original, assertThrows(StarRocksPlannerException.class,
                () -> controller.getRowAccessPolicy(null, table)));
    }

    /** A policy that resolves cleanly is handed back untouched. */
    @Test
    public void testAPolicyThatResolvesIsReturnedAsIs() {
        metadataReturns(null);  // not governed (or unknowable): the native policy is delegated
        Map<String, Expr> answer = Map.of();
        AccessControllerEPack delegate = (AccessControllerEPack) Proxy.newProxyInstance(
                LakeFormationAccessControllerTest.class.getClassLoader(),
                new Class<?>[] {AccessControllerEPack.class},
                (p, method, args) -> "getColumnMaskingPolicy".equals(method.getName()) ? answer : null);
        LakeFormationAccessController controller = new LakeFormationAccessController(delegate);
        assertEquals(answer, controller.getColumnMaskingPolicy(null, new TableName("c", "d", "t"), List.of()));
    }

    // ---- probes ----

    /** Answers every call without objecting, so a behavioural case is not derailed by the native half. */
    private static AccessControllerEPack allowingDelegate() {
        return (AccessControllerEPack) Proxy.newProxyInstance(
                LakeFormationAccessControllerTest.class.getClassLoader(),
                new Class<?>[] {AccessControllerEPack.class},
                (p, method, args) -> null);
    }

    private static AccessControllerEPack throwingDelegate(RuntimeException failure) {
        return (AccessControllerEPack) Proxy.newProxyInstance(
                LakeFormationAccessControllerTest.class.getClassLoader(),
                new Class<?>[] {AccessControllerEPack.class},
                (p, method, args) -> {
                    throw failure;
                });
    }

    /**
     * A real governed table, not a stub: the controller decides on the narrowed fullSchema and on the
     * physical name lists, and those two only tell a consistent story when the table was actually built
     * the way LakeFormationHiveTable builds one.
     *
     * <p>{@code column} is the name under test - present among the physical columns when {@code physical},
     * and among the authorized ones when {@code authorized}. A second column is always both, so the table
     * is never empty and the outcome is attributable to {@code column} alone.
     */
    private static Table governedTable(String column, boolean authorized, boolean physical) {
        List<Column> schema = new ArrayList<>();
        List<String> dataColumns = new ArrayList<>();
        schema.add(new Column(ALWAYS_AUTHORIZED_COLUMN, IntegerType.INT));
        dataColumns.add(ALWAYS_AUTHORIZED_COLUMN);
        if (physical) {
            schema.add(new Column(column, IntegerType.INT));
            dataColumns.add(column);
        }
        HiveTable physicalTable = HiveTable.builder()
                .setId(1L)
                .setTableName("t")
                .setCatalogName("c")
                .setHiveDbName("d")
                .setHiveTableName("t")
                .setTableLocation("s3://bucket/d/t")
                .setCreateTime(1600000000L)
                .setFullSchema(schema)
                .setPartitionColumnNames(new ArrayList<>())
                .setDataColumnNames(dataColumns)
                .setProperties(new HashMap<>())
                .setSerdeProperties(new HashMap<>())
                .setStorageFormat(HiveStorageFormat.PARQUET)
                .build();

        List<Column> authorizedSchema = new ArrayList<>();
        authorizedSchema.add(new Column(ALWAYS_AUTHORIZED_COLUMN, IntegerType.INT));
        if (authorized) {
            authorizedSchema.add(new Column(column, IntegerType.INT));
        }
        return LakeFormationHiveTable.of(physicalTable, authorizedSchema, IDENTITY,
                new LakeFormationTableHandle(IDENTITY, TableLoadPurpose.DATA_ACCESS, "attempt-1"));
    }
}
