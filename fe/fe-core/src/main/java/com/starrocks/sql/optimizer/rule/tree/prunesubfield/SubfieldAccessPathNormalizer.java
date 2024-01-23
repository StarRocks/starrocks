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

package com.starrocks.sql.optimizer.rule.tree.prunesubfield;

import com.google.common.collect.Lists;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.thrift.TAccessPathType;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrTokenizer;

import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/*
 * normalize expression to ColumnAccessPath
 */
public class SubfieldAccessPathNormalizer {
    // todo: BE only support one-layer json path, supported more layer in future
    public static int JSON_FLATTEN_DEPTH = 1;
    // simple json patten, same as BE's JsonPathPiece, match: abc[1][2], group: (abc)([1][2])
    private static final Pattern JSON_ARRAY_PATTEN = Pattern.compile("^([\\w#.]*)((?:\\[[\\d:*]+])*)");

    private final Deque<AccessPath> allAccessPaths = Lists.newLinkedList();

    private static class AccessPath {
        private final ScalarOperator root;
        private final List<String> paths = Lists.newArrayList();
        private final List<TAccessPathType> pathTypes = Lists.newArrayList();

        public AccessPath(ScalarOperator root) {
            this.root = root;
        }

        public AccessPath appendPath(String path, TAccessPathType pathType) {
            paths.add(path);
            pathTypes.add(pathType);
            return this;
        }

        public AccessPath appendFieldNames(Collection<String> fieldNames) {
            fieldNames.forEach(fld -> appendPath(fld, TAccessPathType.FIELD));
            return this;
        }

        public ScalarOperator root() {
            return root;
        }
    }

    public ColumnAccessPath normalizePath(ColumnRefOperator root, String columnName) {
        List<AccessPath> paths = allAccessPaths.stream().filter(path -> path.root().equals(root))
                .sorted((o1, o2) -> Integer.compare(o2.paths.size(), o1.paths.size()))
                .collect(Collectors.toList());

        ColumnAccessPath rootPath = new ColumnAccessPath(TAccessPathType.ROOT, columnName);
        for (AccessPath accessPath : paths) {
            ColumnAccessPath parentPath = rootPath;
            for (int i = 0; i < accessPath.paths.size(); i++) {
                if (parentPath.hasChildPath(accessPath.paths.get(i))) {
                    ColumnAccessPath childPath = parentPath.getChildPath(accessPath.paths.get(i));
                    TAccessPathType pathType = accessPath.pathTypes.get(i);
                    if (childPath.getType() != accessPath.pathTypes.get(i)) {
                        // if the path same but type different, must be PATH_PLACEHOLDER, the Type must be
                        // INDEX, OFFSET, KEY, ALL, and we can merge them
                        // 1. when contains OFFSET and KEY, we set the type to KEY
                        // 2. other (OFFSET-ALL, OFFSET-INDEX, INDEX-KEY, KEY-ALL), we set the type to ALL
                        boolean isOffsetOrKey =
                                childPath.getType() == TAccessPathType.OFFSET && pathType == TAccessPathType.KEY;
                        isOffsetOrKey = isOffsetOrKey ||
                                (childPath.getType() == TAccessPathType.KEY && pathType == TAccessPathType.OFFSET);
                        childPath.setType(isOffsetOrKey ? TAccessPathType.KEY : TAccessPathType.ALL);
                    }
                    parentPath = childPath;
                } else {
                    ColumnAccessPath childPath =
                            new ColumnAccessPath(accessPath.pathTypes.get(i), accessPath.paths.get(i));
                    parentPath.addChildPath(childPath);
                    parentPath = childPath;
                }
            }
            parentPath.clearChildPath();
        }

        return rootPath;
    }

    public boolean hasPath(ColumnRefOperator root) {
        return allAccessPaths.stream().anyMatch(path -> path.root().equals(root));
    }

    private static class Collector extends ScalarOperatorVisitor<Optional<AccessPath>, List<Optional<AccessPath>>> {
        @Override
        public Optional<AccessPath> visit(ScalarOperator scalarOperator,
                                          List<Optional<AccessPath>> childrenAccessPaths) {
            return Optional.empty();
        }

        @Override
        public Optional<AccessPath> visitVariableReference(ColumnRefOperator variable,
                                                           List<Optional<AccessPath>> childrenAccessPaths) {
            if (variable.getType().isComplexType() || variable.getType().isJsonType()) {
                return Optional.of(new AccessPath(variable));
            }
            return Optional.empty();
        }

        @Override
        public Optional<AccessPath> visitSubfield(SubfieldOperator subfieldOperator,
                                                  List<Optional<AccessPath>> childAccessPaths) {
            return childAccessPaths.get(0).map(parent -> parent.appendFieldNames(subfieldOperator.getFieldNames()));
        }

        @Override
        public Optional<AccessPath> visitCollectionElement(CollectionElementOperator collectionElementOp,
                                                           List<Optional<AccessPath>> childrenAccessPaths) {
            Optional<AccessPath> parent = childrenAccessPaths.get(0);
            if (parent.isEmpty()) {
                return Optional.empty();
            }

            if (!collectionElementOp.getChild(1).isConstant()) {
                return parent.map(p -> p.appendPath(ColumnAccessPath.PATH_PLACEHOLDER, TAccessPathType.ALL));
            } else {
                return parent.map(p -> p.appendPath(ColumnAccessPath.PATH_PLACEHOLDER, TAccessPathType.INDEX));
            }
        }

        @Override
        public Optional<AccessPath> visitCall(CallOperator call, List<Optional<AccessPath>> childrenAccessPaths) {
            if (!PruneSubfieldRule.SUPPORT_FUNCTIONS.contains(call.getFnName())) {
                return Optional.empty();
            }

            if (call.getFnName().equals(FunctionSet.MAP_KEYS)) {
                return childrenAccessPaths.get(0)
                        .map(p -> p.appendPath(ColumnAccessPath.PATH_PLACEHOLDER, TAccessPathType.KEY));
            } else if (FunctionSet.MAP_SIZE.equals(call.getFnName())
                    || FunctionSet.CARDINALITY.equals(call.getFnName())
                    || FunctionSet.ARRAY_LENGTH.equals(call.getFnName())) {
                return childrenAccessPaths.get(0)
                        .map(p -> p.appendPath(ColumnAccessPath.PATH_PLACEHOLDER, TAccessPathType.OFFSET));
            } else if (PruneSubfieldRule.SUPPORT_JSON_FUNCTIONS.contains(call.getFnName())
                    && call.getArguments().size() > 1 && call.getArguments().get(1).isConstantRef()) {

                String path = ((ConstantOperator) call.getArguments().get(1)).getVarchar();
                // we flatten whole json path, and control the query hierarchy dynamically through BE-self
                return childrenAccessPaths.get(0).map(p -> p.appendFieldNames(formatJsonPath(path)));
            }

            return Optional.empty();
        }

        // format json path, same as BE's JsonPathPiece, just supported simple path for prune subfield
        // split char: .
        // escape char: \
        // quota char: "
        //
        // eg.
        //  $.a.b -> [a, b]
        //  $.a[0].b -> [a] -- don't support array index
        //  $."a.b".c -> ["a.b", c]
        //  $.a#b.c -> [a#b, c]
        //  $.a.b.c.d.e.f -> [a, b] -- don't support overflown JSON_FLATTEN_DEPTH
        //  a.b.c -> [a, b, c]
        // when meet some unsupported path, return null
        public static List<String> formatJsonPath(String path) {
            path = StringUtils.trimToEmpty(path);
            if (StringUtils.isBlank(path) || StringUtils.contains(path, "..") || StringUtils.equals("$", path)) {
                // .. is recursive search in json path, not supported
                return Collections.emptyList();
            }
            if (StringUtils.countMatches(path, "\"") % 2 != 0) {
                // unpaired quota char
                return Collections.emptyList();
            }
            
            StrTokenizer tokenizer = new StrTokenizer(path, '.', '"');
            String[] tokens = tokenizer.getTokenArray();

            if (tokens.length < 1) {
                return Collections.emptyList();
            }
            int size = JSON_FLATTEN_DEPTH;
            List<String> result = Lists.newArrayList();

            int i = 0;
            if (tokens[0].equals("$")) {
                size++;
                i++;
            }
            size = Math.min(tokens.length, size);
            for (; i < size; i++) {
                if (tokens[i].contains(".")) {
                    result.add("\"" + tokens[i] + "\"");
                    continue;
                }
                // unsupported path, should stop match
                Matcher matcher = JSON_ARRAY_PATTEN.matcher(tokens[i]);
                if (!matcher.matches()) {
                    break;
                }
                // only extract name, don't needed index
                String name = matcher.group(1);
                result.add(name);
                if (tokens[i].replaceFirst(name, "").contains("[")) {
                    // can't support flatten array index
                    break;
                }
            }
            return result;
        }

        private Optional<AccessPath> process(ScalarOperator scalarOperator, Deque<AccessPath> accessPaths) {
            // process children in post-order
            List<Optional<AccessPath>> childAccessPaths = scalarOperator.getChildren().stream()
                    .map(child -> process(child, accessPaths))
                    .collect(Collectors.toList());
            // no AccessPaths gathered from children of intermediate ScalarOperator means current
            // scalar operator contains not nested types.
            if (!childAccessPaths.isEmpty() && childAccessPaths.stream().noneMatch(Optional::isPresent)) {
                return Optional.empty();
            }
            Optional<AccessPath> currentPath = scalarOperator.accept(this, childAccessPaths);
            AccessPath path = currentPath.orElse(null);
            // When an AccessPath from offspring ScalarOperators can be extended to a longer AccessPath
            // in current ScalarOperator would not gathered until it can not be extended.
            // Since AccessPath is extended by appending path component in-place, so AccessPaths in
            // childAccessPaths that is not identical to AccessPath of the current ScalarOperator is
            // non-extendable.
            childAccessPaths.stream().filter(p -> p.isPresent() && p.get() != path)
                    .map(Optional::get).forEach(accessPaths::add);
            return currentPath;
        }
    }

    public void collect(List<ScalarOperator> scalarOperators) {
        Collector collector = new Collector();
        List<Optional<AccessPath>> paths =
                scalarOperators.stream().map(op -> collector.process(op, allAccessPaths)).collect(Collectors.toList());
        paths.forEach(p -> p.ifPresent(allAccessPaths::add));
    }
}
