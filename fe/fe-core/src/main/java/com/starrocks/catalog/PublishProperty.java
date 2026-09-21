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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TPrimitiveType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The properties a CN reads while publishing a version: which ones exist, what each may hold, and
 * what one table currently carries.
 *
 * <p>An instance is one table's set together with the revision that names it. The two belong to the
 * same object because a CN caches by revision and does not re-read a revision it has already seen:
 * were a reader ever to observe a new revision beside the previous set, the CN would cache that
 * pairing and keep serving it until the next ALTER. So a change builds a whole new instance rather
 * than editing one in place, and a reader on the publish path -- which holds no metadata lock --
 * sees either the complete old one or the complete new one.
 *
 * <p>The static half decides which names travel. A name absent from it is unknown to every statement,
 * so a typo is rejected where the user typed it. Only properties a publish can act on belong there:
 * anything a CN consumes outside publish, and anything sizing a process-wide resource -- thread
 * pools, queue lengths, memory watermarks -- stays CN config, because a per-table value for those has
 * no meaning. A CN reads the values through whichever object consumes them and ignores the rest, so a
 * name registered here that no CN read site claims is accepted, stored, shown, and does nothing; what
 * catches that is the end-to-end test each property ships with.
 *
 * <p>An instance is derived from the table's stored properties and is not itself persisted. The
 * revision is, because a CN that has cached revision 7 rejects everything below it, so a restart that
 * reset the count would strand every later ALTER.
 */
public class PublishProperty {
    /**
     * The revision of a table that has never carried a publish property.
     *
     * <p>A table that once carried one keeps a higher revision even after every property is removed,
     * and that is what tells the two apart: the first has nothing for a CN to forget, while the second
     * has values a CN is still serving and must be told to drop.
     */
    public static final long INITIAL_REVISION = 0;

    /**
     * What a table that has never carried a publish property has.
     *
     * <p>Also what a publish path passes when it could not read the table at all -- one dropped out
     * from under it, or one built without any property -- so that the path hands on something a
     * publish can read rather than a null every method below it would have to remember to guard.
     * Immutable, so one instance serves every such table.
     */
    public static final PublishProperty NEVER_SET = new PublishProperty(INITIAL_REVISION, Map.of());

    /**
     * One property, and the tables and values it accepts.
     *
     * <p>{@code runModes} and {@code keysTypes} are independent requirements: a table has to match
     * both, and each reports its own failure so a rejected statement says which one it missed.
     * {@code runModes} is matched against the table's own storage -- a cloud-native table counts as
     * shared_data, every other table as shared_nothing -- and not against the cluster's run mode, so
     * a shared-data cluster cannot slip a property onto a table whose tablets live elsewhere.
     *
     * <p>{@code minValue} and {@code maxValue} are the closed range a statement may set. The range is
     * meant to catch a typo -- a value with one zero too many, or a byte count typed in megabytes --
     * not to decide what is a sensible setting, because a table must not be held to a narrower range
     * than be.conf. Zero is inside the range only where a read site treats it as a switch.
     *
     * <p>{@code type} is the width a CN reads the value at. Every value parses as a long here, so the
     * range check already rejects anything a narrower read site could not hold; the self-check below
     * keeps the two in step by refusing a spec whose range overflows its own type.
     *
     * <p>{@code unsetValue}, when present, is the literal that removes the property and returns the
     * table to the CN's own config value. A spec that leaves it empty has no unset form: every literal
     * it accepts is a value, and the property can only be overwritten, never taken back off.
     */
    private record Spec(String name, TPrimitiveType type, long minValue, long maxValue,
                        Set<RunMode> runModes, Set<KeysType> keysTypes, Optional<String> unsetValue) {
    }

    /** What one statement does to a table's publish properties. */
    public record Changes(Map<String, String> upserts, Set<String> removals) {
        public boolean isEmpty() {
            return upserts.isEmpty() && removals.isEmpty();
        }
    }

    private static final Set<RunMode> SHARED_DATA_ONLY = Set.of(RunMode.SHARED_DATA);
    private static final Set<KeysType> PRIMARY_KEY_ONLY = Set.of(KeysType.PRIMARY_KEYS);

    private static final Map<String, Spec> SPECS = buildSpecs();

    private static Map<String, Spec> buildSpecs() {
        Map<String, Spec> specs = new LinkedHashMap<>();

        // The two factors of a primary key index's memtable budget: at most max_count memtables of
        // max_bytes each.
        declare(specs, "pk_index_memtable_max_count", TPrimitiveType.INT, 1, 64);
        declare(specs, "pk_index_memtable_max_bytes", TPrimitiveType.BIGINT, 1, 4294967296L);

        // How much index replay may pile up before a publish spends an extra flush to shorten a
        // future rebuild. Both read sites ignore the threshold when it is not positive, so 0 is how a
        // table turns the trigger off -- which is not the same as leaving the property unset.
        declare(specs, "pk_index_rebuild_files_threshold", TPrimitiveType.INT, 0, 100000);
        declare(specs, "pk_index_rebuild_rows_threshold", TPrimitiveType.BIGINT, 0, 10000000000L);

        // Where a segment's primary keys are cut into batches: whichever of the two trips first ends
        // the batch, and each batch is one parallel task.
        declare(specs, "pk_index_parallel_execution_min_rows", TPrimitiveType.BIGINT, 1, 100000000L);
        declare(specs, "pk_column_read_batch_bytes", TPrimitiveType.BIGINT, 1, 4294967296L);

        // Reading the compaction rows mapper: how many reads stay in flight, and how large each is.
        // Their product is the memory this read holds.
        declare(specs, "pk_rows_mapper_read_parallelism", TPrimitiveType.INT, 1, 256);
        declare(specs, "pk_rows_mapper_read_batch_bytes", TPrimitiveType.BIGINT, 1, 67108864L);

        // How many rows accumulate before one replace call into the primary key index. One means no
        // accumulation, which is the behavior from before batching existed.
        declare(specs, "pk_compaction_replace_batch_rows", TPrimitiveType.INT, 1, 10000000);

        return ImmutableMap.copyOf(specs);
    }

    // Whether a value survives being read back at |type|. Only the two widths a CN reads publish
    // properties at are answerable; a spec declaring anything else is a programming error, and the
    // caller below is where it surfaces.
    private static boolean holds(TPrimitiveType type, long value) {
        return switch (type) {
            case INT -> value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE;
            case BIGINT -> true;
            default -> throw new IllegalStateException("publish property declares unsupported type " + type);
        };
    }

    // Every property in the first batch removes on the empty string. A spec declares its own rather
    // than sharing one constant, because a property whose values include the empty string would have
    // nothing left to mean "remove me".
    private static void declare(Map<String, Spec> specs, String name, TPrimitiveType type, long minValue,
                                long maxValue) {
        declare(specs, name, type, minValue, maxValue, Optional.of(""));
    }

    private static void declare(Map<String, Spec> specs, String name, TPrimitiveType type, long minValue,
                                long maxValue, Optional<String> unsetValue) {
        // A range its own type cannot hold would be accepted here and then truncated at the CN,
        // where nothing would report it.
        if (!holds(type, minValue) || !holds(type, maxValue) || minValue > maxValue) {
            throw new IllegalStateException("publish property " + name + " declares range [" + minValue + ", "
                    + maxValue + "], which " + type + " cannot hold");
        }
        specs.put(name, new Spec(name, type, minValue, maxValue, SHARED_DATA_ONLY, PRIMARY_KEY_ONLY, unsetValue));
    }

    /**
     * True when any name in {@code properties} is a publish property.
     *
     * <p>This answers the routing question -- whether the statement belongs on the publish property
     * path at all -- and not whether every name on it is one. A statement mixing a publish property
     * with something else still has to come this way, so that the mixed name is reported by the check
     * that knows what it is rather than as an unknown property.
     */
    public static boolean declaresAny(Map<String, String> properties) {
        return properties != null && properties.keySet().stream().anyMatch(SPECS::containsKey);
    }

    /**
     * The changes {@code properties} describes, read the way they were written: a value sets the
     * property, and the literal a property declares as its unset form removes it instead.
     *
     * <p>Nothing is checked, and nothing is taken out of {@code properties}. That is what reading an
     * edit log entry needs: its values were checked when the statement ran, and checking them again
     * here would let a range narrowed in a later version stop a replay -- and with it, FE startup.
     */
    public static Changes parseChanges(Map<String, String> properties) {
        Map<String, String> upserts = new LinkedHashMap<>();
        Set<String> removals = new LinkedHashSet<>();
        if (properties == null) {
            return new Changes(upserts, removals);
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            Spec spec = SPECS.get(entry.getKey());
            if (spec == null) {
                continue;
            }
            if (spec.unsetValue().isPresent() && spec.unsetValue().get().equals(entry.getValue())) {
                removals.add(spec.name());
            } else {
                upserts.put(spec.name(), entry.getValue());
            }
        }
        return new Changes(upserts, removals);
    }

    /**
     * The changes {@code properties} describes, checked against what each property accepts and against
     * {@code table}, and then taken out of the map so the caller's leftover check sees only names
     * nobody recognized.
     *
     * @throws com.starrocks.sql.analyzer.SemanticException if a value does not parse, falls outside
     *         the property's range, or {@code table} does not match its run mode or keys type
     */
    public static Changes validateAndExtract(Map<String, String> properties, OlapTable table) {
        Changes changes = parseChanges(properties);
        for (Map.Entry<String, String> entry : changes.upserts().entrySet()) {
            Spec spec = SPECS.get(entry.getKey());
            checkScope(spec, table);
            checkValue(spec, entry.getValue());
        }
        for (String removed : changes.removals()) {
            checkScope(SPECS.get(removed), table);
        }
        if (properties != null) {
            properties.keySet().removeIf(SPECS::containsKey);
        }
        return changes;
    }

    /** The publish properties among {@code properties}, in declaration order. */
    public static Map<String, String> selectFrom(Map<String, String> properties) {
        Map<String, String> found = new LinkedHashMap<>();
        if (properties == null) {
            return found;
        }
        for (String name : SPECS.keySet()) {
            String value = properties.get(name);
            if (value != null) {
                found.put(name, value);
            }
        }
        return found;
    }

    public static Set<String> names() {
        return SPECS.keySet();
    }

    private static void checkScope(Spec spec, OlapTable table) {
        // The table's own storage decides, not the cluster's run mode. A shared-data cluster can hold
        // a table whose tablets are not cloud-native -- an external OLAP table points at a separate
        // shared-nothing cluster -- and that table would otherwise accept a property its tablets
        // never see.
        RunMode tableRunMode = table.isCloudNativeTable() ? RunMode.SHARED_DATA : RunMode.SHARED_NOTHING;
        if (!spec.runModes().contains(tableRunMode)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Property " + spec.name() + " is only supported for "
                            + spec.runModes().stream().map(RunMode::getName).sorted().collect(Collectors.joining(", "))
                            + " tables, but this table is " + tableRunMode.getName());
        }
        if (!spec.keysTypes().contains(table.getKeysType())) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Property " + spec.name() + " is only supported for "
                            + spec.keysTypes().stream().map(KeysType::toSql).sorted().collect(Collectors.joining(", "))
                            + " tables");
        }
    }

    private static void checkValue(Spec spec, String value) {
        long parsed;
        try {
            parsed = Long.parseLong(value);
        } catch (NumberFormatException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Property " + spec.name() + " must be an integer: " + value);
            return;
        }
        if (parsed < spec.minValue() || parsed > spec.maxValue()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Property " + spec.name() + " must be between " + spec.minValue() + " and "
                            + spec.maxValue() + ": " + value);
        }
    }

    private final long revision;
    private final Map<String, String> properties;

    public PublishProperty(long revision, Map<String, String> properties) {
        this.revision = revision;
        // Copied, so that the map the caller built and still holds cannot become this instance's
        // contents afterwards.
        this.properties = Collections.unmodifiableMap(Maps.newLinkedHashMap(properties));
    }

    /**
     * Whether this belongs to a table that has never carried a publish property, and so has nothing a
     * publish needs to send.
     */
    public boolean isNeverSet() {
        return revision == INITIAL_REVISION;
    }

    public long getRevision() {
        return revision;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
