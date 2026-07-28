// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.google.common.collect.ImmutableSet;
import com.starrocks.persist.IgnorableOnReplayFailed;
import com.starrocks.persist.OperationType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

public class OperationTypeEPack extends OperationType {
    private static final Logger LOG = LogManager.getLogger(OperationTypeEPack.class);

    @IgnorableOnReplayFailed
    public static final short OP_CREATE_MASKING_POLICY = 20001;
    @IgnorableOnReplayFailed
    public static final short OP_CREATE_ROW_ACCESS_POLICY = 20002;
    @IgnorableOnReplayFailed
    public static final short OP_DROP_POLICY = 20003;
    @IgnorableOnReplayFailed
    public static final short OP_ALTER_POLICY_SET_BODY = 20004;
    @IgnorableOnReplayFailed
    public static final short OP_ALTER_POLICY_SET_COMMENT = 20005;
    @IgnorableOnReplayFailed
    public static final short OP_ALTER_POLICY_RENAME = 20006;
    @IgnorableOnReplayFailed
    public static final short OP_APPLY_MASKING_POLICY = 20007;
    @IgnorableOnReplayFailed
    public static final short OP_APPLY_ROW_ACCESS_POLICY = 20008;
    @IgnorableOnReplayFailed
    public static final short OP_REVOKE_MASKING_POLICY = 20009;
    @IgnorableOnReplayFailed
    public static final short OP_REVOKE_ROW_ACCESS_POLICY = 20010;

    @IgnorableOnReplayFailed
    public static final short OP_CREATE_PASSWORD_POLICY = 20020;
    @IgnorableOnReplayFailed
    public static final short OP_DROP_PASSWORD_POLICY = 20021;
    @IgnorableOnReplayFailed
    public static final short OP_SET_PASSWORD_POLICY = 20022;
    @IgnorableOnReplayFailed
    public static final short OP_UNSET_PASSWORD_POLICY = 20023;

    // warehouse
    //public static final short OP_CREATE_WAREHOUSE = 20101;
    //public static final short OP_DROP_WAREHOUSE = 20102;
    //public static final short OP_ALTER_WAREHOUSE = 20103;

    // security integration and role mapping
    //public static final short OP_CREATE_SECURITY_INTEGRATION = 20269;
    //public static final short OP_DROP_SECURITY_INTEGRATION = 20271;
    //public static final short OP_ALTER_SECURITY_INTEGRATION = 20272;
    @IgnorableOnReplayFailed
    public static final short OP_CREATE_ROLE_MAPPING = 20270;
    @IgnorableOnReplayFailed
    public static final short OP_DROP_ROLE_MAPPING = 20273;
    @IgnorableOnReplayFailed
    public static final short OP_ALTER_ROLE_MAPPING = 20274;

    // failover group
    @IgnorableOnReplayFailed
    public static final short OP_CREATE_FAILOVER_GROUP = 20301;
    @IgnorableOnReplayFailed
    public static final short OP_DROP_FAILOVER_GROUP = 20302;
    @IgnorableOnReplayFailed
    public static final short OP_UPDATE_FAILOVER_GROUP = 20303;

    // AutoMV
    @IgnorableOnReplayFailed
    public static final short OP_MV_CHANGE = 20401;
    @IgnorableOnReplayFailed
    public static final short OP_RECOMMENDATIONS_TASK_STATUS_CHANGE = 20402;

    // Grant Role to Group
    //public static final short OP_GRANT_ROLE_TO_GROUP = 20501;
    //public static final short OP_REVOKE_ROLE_FROM_GROUP = 20502;

    // Manual Cluster Snapshot
    @IgnorableOnReplayFailed
    public static final short OP_MANUAL_CLUSTER_SNAPSHOT_LOG = 20601;
    // Deliberately NOT ignorable: restoring from a cluster snapshot is a critical recovery step,
    // so a failed replay must halt the FE rather than continue with a half-applied restore.
    public static final short OP_RESTORE_FROM_SNAPSHOT = 20602;

    // License
    @IgnorableOnReplayFailed
    public static final short OP_INIT_SYSTEM_INFO = 20610;
    @IgnorableOnReplayFailed
    public static final short OP_REGISTER_LICENSE = 20611;
    @IgnorableOnReplayFailed
    public static final short OP_UPDATE_SCALE_OUT_LICENSE_FREE_START_TIME = 20612;

    /**
     * Union of the community ignorable ops and the EE ones. Replay-failure skip checks must
     * consult THIS set instead of {@link OperationType#IGNORABLE_OPERATIONS}: the community
     * builder scans {@code OperationType.class} only and can never see the EE operation types
     * declared in this subclass.
     */
    public static final ImmutableSet<Short> IGNORABLE_OPERATIONS = buildIgnorableOperations();

    private static ImmutableSet<Short> buildIgnorableOperations() {
        ImmutableSet.Builder<Short> builder = ImmutableSet.builder();
        Set<Short> allOperations = new HashSet<>();

        // getFields() returns the EE ops declared here plus the inherited community ones, so the
        // resulting set covers both and the duplicate check catches community/EE collisions.
        for (Field field : OperationTypeEPack.class.getFields()) {
            if (!field.getName().startsWith("OP_")) {
                continue;
            }
            short opType = Short.MIN_VALUE;
            try {
                opType = (short) field.get(null);
            } catch (IllegalAccessException e) {
                LOG.fatal("get value from {} failed, will exit.", field.getName(), e);
                System.exit(-1);
            }

            // Community fields are validated by OperationType's own builder; EE fields must stay
            // in the range reserved above OP_TYPE_EOF so they never collide with community ops.
            if (field.getDeclaringClass() == OperationTypeEPack.class && opType <= OperationType.OP_TYPE_EOF) {
                LOG.fatal("OperationTypeEPack must use a value exceeding {}: {} = {}",
                        OperationType.OP_TYPE_EOF, field.getName(), opType);
                System.exit(-1);
            }

            if (!allOperations.add(opType)) {
                LOG.fatal("Duplicate operation type {} with value {}, will exit.", field.getName(), opType);
                System.exit(-1);
            }

            Annotation annotation = field.getAnnotation(IgnorableOnReplayFailed.class);
            if (annotation != null) {
                builder.add(opType);
            }
        }

        return builder.build();
    }
}
