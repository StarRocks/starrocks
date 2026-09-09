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

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.starrocks.planner.expression.ExprToThrift;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.rule.tree.prunesubfield.SubfieldAccessPathNormalizer;
import com.starrocks.thrift.TAccessPathType;
import com.starrocks.thrift.TColumnAccessPath;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeDeserializer;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.TypeSerializer;

import java.util.List;
import java.util.stream.Collectors;

/*
 * ColumnAccessPath is used to describe the access path of a complex(Map/Struct/Json) column.
 *
 * eg:
 *  select struct_a.col_b.col_c.col_d
 *
 * ColumnAccessPath will be:
 *  struct_a (ROOT)
 *  -- col_b (FIELD)
 *    -- col_c (FIELD)
 *      -- col_d (FIELD)
 *
 * in complex sql, eg:
 *  select  struct_a.col_g, struct_a.col_b.col_c.col_d, struct_a.col_b.col_e.col_f
 * ColumnAccessPath will be:
 *  struct_a (ROOT)
 *  -- col_g (FIELD)
 *  -- col_b (FIELD)
 *    -- col_c (FIELD)
 *      -- col_d (FIELD)
 *    -- col_e (FIELD)
 *      -- col_f (FIELD)
 *
 */
public class ColumnAccessPath {
    public static final String PATH_PLACEHOLDER = "P";
    // The top one must be ROOT
    private TAccessPathType type;

    private final String path;

    private final List<ColumnAccessPath> children;

    private boolean fromPredicate;

    // Extended access path from json predicate
    // WHERE get_json_int(c1, 'f1') > 100 => c1.f1 > 100
    // Along with the expression transformation, it will generate an extended AccessPath
    private boolean extended;

    // flat json used, to mark the type of the leaf
    private Type valueType;

    public ColumnAccessPath(TAccessPathType type, String path, Type valueType) {
        this.type = type;
        this.path = path;
        this.children = Lists.newArrayList();
        this.fromPredicate = false;
        this.extended = false;
        this.valueType = normalizeStorageValueType(valueType);
    }

    /**
     * The value type recorded on an access path is an <em>expression</em> type -- typically the target
     * type of a cast such as {@code CAST(json_col->'$.s' AS char)} -- but BE consumes it as a
     * <em>storage</em> column type: it becomes the type (and declared width) of the synthetic
     * TabletColumn that materializes the flat-JSON / VARIANT subfield, and the leaf type the JSON
     * merger dispatches on. The two layers do not accept the same shapes:
     *
     * <ul>
     *   <li>A length-less CHAR/VARCHAR carries the wildcard sentinel {@code -1}
     *       ({@link ScalarType#isWildcardChar()}), which is legal and meaningful in the expression
     *       world but reaches BE as an {@code int32} that downstream code reads unsigned, i.e.
     *       4294967295.</li>
     *   <li>CHAR has no storage counterpart on this path at all: the flat subfield on disk is always
     *       VARCHAR, and BE's own cast factory rewrites CHAR to VARCHAR before evaluating
     *       ({@code be/src/exprs/cast_expr.cpp}), so the CHAR only survives as a storage type that
     *       parts of the storage layer are not prepared for.</li>
     * </ul>
     *
     * <p>Normalizing here -- the single place a value type is recorded -- keeps the expression world
     * untouched (the cast still reports CHAR to the user) while the storage world only ever sees a
     * plain, properly-sized VARCHAR.</p>
     *
     * <p>The declared width is deliberately dropped rather than clamped, unlike
     * {@code AnalyzerUtils#transformTableColumnType} which keeps {@code min(len, max)} when it
     * materializes a real column. Here the width has never been enforced: BE reads the subfield
     * through the string reader and ignores it, so {@code CAST(j->'$.s' AS char(3))} does not
     * truncate a stored subfield. The one place the width was read is
     * {@code DefaultValueColumnIterator}'s CHAR branch, and there it truncated -- which is a
     * behaviour {@code CAST} is not supposed to have anywhere in StarRocks
     * ({@code be/src/exprs/cast_expr_tpl.hpp}: "neglect of the length of char/varchar and return
     * input column directly"). Clamping to {@code min(len, max)} would preserve exactly that one
     * wrong behaviour; taking the max drops the width the way every other string cast does.</p>
     *
     * <p>Callers outside this class use it to compare two recorded value types on the same
     * footing: an already-normalized one against a raw one would differ by primitive type and
     * degrade the merged path to JSON.</p>
     */
    public static Type normalizeStorageValueType(Type valueType) {
        if (!(valueType instanceof ScalarType scalarType)) {
            return valueType;
        }
        PrimitiveType primitiveType = scalarType.getPrimitiveType();
        if (primitiveType != PrimitiveType.CHAR && primitiveType != PrimitiveType.VARCHAR) {
            return valueType;
        }
        int maxLength = TypeFactory.getOlapMaxVarcharLength();
        if (primitiveType == PrimitiveType.VARCHAR && scalarType.getLength() == maxLength) {
            // already the storage shape, don't allocate on this per-query path
            return valueType;
        }
        // Must build a new type: CharType.CHAR and VarcharType.VARCHAR are shared singletons and
        // ScalarType#setLength mutates in place, so normalizing by setLength would corrupt the
        // wildcard types process-wide.
        return TypeFactory.createVarcharType(maxLength);
    }

    /**
     * Create a linear path like a.b.c, one node has at most one child node
     */
    public static ColumnAccessPath createLinearPath(List<String> path, Type valueType) {
        Preconditions.checkArgument(path != null && !path.isEmpty(), "Path must not be empty");
        ColumnAccessPath root = new ColumnAccessPath(TAccessPathType.ROOT, path.get(0), valueType);
        ColumnAccessPath curr = root;
        for (String field : path.subList(1, path.size())) {
            ColumnAccessPath node = new ColumnAccessPath(TAccessPathType.FIELD, field, valueType);
            curr.addChildPath(node);
            curr = node;
        }
        return root;
    }

    public static ColumnAccessPath createFromLinearPath(String linearPath, Type valueType) {
        List<String> pieces = SubfieldAccessPathNormalizer.parseSimpleJsonPath(linearPath);
        if (pieces.isEmpty()) {
            throw new IllegalArgumentException("illegal json path: " + linearPath);
        }
        return createLinearPath(pieces, valueType);
    }

    public static ColumnAccessPath fromThrift(TColumnAccessPath thrift) {
        if (thrift == null || thrift.type == null) {
            throw new IllegalArgumentException("column access path misses type");
        }
        Type type = thrift.isSetType_desc() ? TypeDeserializer.fromThrift(thrift.type_desc) : InvalidType.INVALID;
        ColumnAccessPath path = new ColumnAccessPath(thrift.type, getPathFromThrift(thrift), type);
        if (thrift.isSetFrom_predicate()) {
            path.setFromPredicate(thrift.from_predicate);
        }
        if (thrift.isSetExtended()) {
            path.setExtended(thrift.extended);
        }
        if (thrift.children != null) {
            thrift.children.stream().map(ColumnAccessPath::fromThrift).forEach(path::addChildPath);
        }
        return path;
    }

    private static String getPathFromThrift(TColumnAccessPath thrift) {
        if (thrift.path == null || thrift.path.nodes == null || thrift.path.nodes.size() != 1) {
            throw new IllegalArgumentException("column access path must be a string literal");
        }
        TExprNode node = thrift.path.nodes.get(0);
        if (node.node_type != TExprNodeType.STRING_LITERAL || node.string_literal == null) {
            throw new IllegalArgumentException("column access path must be a string literal");
        }
        return node.string_literal.value;
    }

    /**
     * Return the string representation of linear path like a.b.c
     */
    public String getLinearPath() {
        StringBuilder sb = new StringBuilder();
        ColumnAccessPath iter = this;
        while (iter != null) {
            if (!sb.isEmpty()) {
                sb.append(".");
            }
            sb.append(iter.getPath());
            if (!iter.children.isEmpty()) {
                assert iter.children.size() == 1;
                iter = iter.children.get(0);
            } else {
                iter = null;
            }
        }
        return sb.toString();
    }

    public void setType(TAccessPathType type) {
        this.type = type;
    }

    public TAccessPathType getType() {
        return type;
    }

    public String getPath() {
        return path;
    }

    public boolean onlyRoot() {
        return type == TAccessPathType.ROOT && children.isEmpty();
    }

    public void setValueType(Type valueType) {
        this.valueType = normalizeStorageValueType(valueType);
    }

    public Type getValueType() {
        return valueType;
    }

    public void setFromPredicate(boolean fromPredicate) {
        this.fromPredicate = fromPredicate;
    }

    public boolean isFromPredicate() {
        return fromPredicate;
    }

    public boolean isExtended() {
        return extended;
    }

    public void setExtended(boolean extended) {
        this.extended = extended;
    }

    public boolean hasChildPath(String path) {
        return children.stream().anyMatch(p -> p.path.equals(path));
    }

    public boolean hasOverlap(List<String> fieldNames) {
        if (!hasChildPath() || fieldNames.isEmpty()) {
            return true;
        }

        if (children.stream().noneMatch(p -> p.path.equals(fieldNames.get(0)))) {
            return false;
        }
        return getChildPath(fieldNames.get(0)).hasOverlap(fieldNames.subList(1, fieldNames.size()));
    }

    public boolean hasChildPath() {
        return !children.isEmpty();
    }

    public void addChildPath(ColumnAccessPath child) {
        children.add(child);
    }

    public ColumnAccessPath getChildPath(String path) {
        return children.stream().filter(p -> p.path.equals(path)).findFirst().orElse(null);
    }

    public List<ColumnAccessPath> getChildren() {
        return children;
    }

    public void clearChildPath() {
        children.clear();
    }

    public void clearUnusedValueType() {
        // only save leaf's value type
        if (!children.isEmpty()) {
            this.valueType = InvalidType.INVALID;
            children.forEach(ColumnAccessPath::clearUnusedValueType);
        }
    }

    private void explainImpl(String parent, List<String> allPaths) {
        boolean hasName = type == TAccessPathType.FIELD || type == TAccessPathType.ROOT;
        boolean hasType = valueType != InvalidType.INVALID;
        String cur = parent + "/" + (hasName ? path : type.name())
                + (hasType ? "(" + valueType.toSql() + ")" : "");
        if (children.isEmpty()) {
            allPaths.add(cur);
        }
        for (ColumnAccessPath child : children) {
            child.explainImpl(cur, allPaths);
        }
    }

    public String explain() {
        List<String> allPaths = Lists.newArrayList();
        explainImpl("", allPaths);
        allPaths.sort(String::compareTo);
        return String.join(", ", allPaths);
    }

    @Override
    public String toString() {
        return path;
    }

    public TColumnAccessPath toThrift() {
        TColumnAccessPath tColumnAccessPath = new TColumnAccessPath();
        tColumnAccessPath.setType(type);
        tColumnAccessPath.setPath(ExprToThrift.treeToThrift(new StringLiteral(path)));
        tColumnAccessPath.setChildren(children.stream().map(ColumnAccessPath::toThrift).collect(Collectors.toList()));
        tColumnAccessPath.setFrom_predicate(fromPredicate);
        tColumnAccessPath.setExtended(extended);
        if (valueType != null) {
            tColumnAccessPath.setType_desc(TypeSerializer.toThrift(valueType));
        }
        return tColumnAccessPath;
    }
}
