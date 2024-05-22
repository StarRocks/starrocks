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

package com.starrocks.sql.automv.pieces;

import com.google.common.collect.Queues;
import com.starrocks.sql.automv.util.PrettyPrinter;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PlanPiecePrinter {

    public static String print(PlanPiece planPiece) {
        planPiece.assignPieceIds();
        return new PlanPieceFormatter().print(planPiece, null, Integer.MAX_VALUE).getResult();
    }

    public static PrettyPrinter print(PlanPiece planPiece, PrettyPrinter prettyPrinter, int level) {
        planPiece.assignPieceIds();
        return new PlanPieceFormatter().print(planPiece, prettyPrinter, level);
    }

    private static class Context {
        final PrettyPrinter prettyPrinter;
        final Queue<Integer> indents;

        private Integer currentId;

        public Context(PrettyPrinter prettyPrinter) {
            this.prettyPrinter = Objects.requireNonNull(prettyPrinter);
            this.indents = Queues.newArrayDeque();
        }

        public PrettyPrinter getPrinter() {
            return prettyPrinter;
        }

        public int indent() {
            return indents.remove();
        }

        public void addIndent(int n) {
            indents.add(n);
        }
    }

    private static class PlanPieceFormatter extends PlanPieceVisitor<Void, Context> {

        private void formatTittle(PlanPiece piece, Context context) {
            int pieceId = piece.getAuxState().getId();
            String pieceName = piece.getClass().getSimpleName();
            PrettyPrinter printer = context.getPrinter();
            printer.newLine();
            printer.add("[").add(pieceId).add("]:").spaces(1).add(pieceName).newLine();
            List<Integer> inputPieceIds =
                    piece.getInputPieces().stream().map(p -> p.getAuxState().getId()).collect(Collectors.toList());
            printer.indentEnclose(() -> {
                printer.add("InputPieces: [").addItems(", ", inputPieceIds).add("]").newLine();
            });
        }

        private void formatColumns(PlanPiece piece, Context context) {
            PrettyPrinter printer = context.getPrinter();
            printer.indentEnclose(() -> {
                printer.add("Columns:").newLine();
                piece.getColumns().format("Columns", printer);
            });
        }

        private void formatConjuncts(PlanPiece piece, Context context) {
            PrettyPrinter printer = context.getPrinter();
            printer.indentEnclose(() -> {
                printer.add("Conjuncts:").newLine();
                piece.getConjuncts().format("Conjuncts", printer);
            });
        }

        @Override
        public Void visit(PlanPiece piece, Context context) {
            formatColumns(piece, context);
            formatConjuncts(piece, context);
            return null;
        }

        @Override
        public Void visitTable(TablePiece tablePiece, Context context) {
            formatTittle(tablePiece, context);
            PrettyPrinter printer = context.getPrinter();
            printer.indentEnclose(() -> {
                printer.add("TableName: ").add(tablePiece.getTableName()).newLine();
            });
            return visit(tablePiece, context);
        }

        @Override
        public Void visitAggregate(AggregatePiece aggPiece, Context context) {
            formatTittle(aggPiece, context);
            PrettyPrinter printer = context.getPrinter();
            printer.indentEnclose(() -> {
                printer.add("Dimensions:").newLine();
                aggPiece.getDimensions().format("Dimensions", printer);
                printer.add("RollupDimensions:").newLine();
                aggPiece.getRollupDimensions().format("RollupDimensions", printer);
                printer.add("Metrics:").newLine();
                aggPiece.getMetrics().format("Metrics", printer);
                printer.add("DistinctMetrics:").newLine();
                aggPiece.getDistinctMetrics().format("DistinctMetrics", printer);
            });
            aggPiece.getInputPieces().forEach(input -> context.addIndent(0));
            return visit(aggPiece, context);
        }

        @Override
        public Void visitStarJoin(StarJoinPiece joinPiece, Context context) {
            formatTittle(joinPiece, context);
            PrettyPrinter printer = context.getPrinter();
            printer.indentEnclose(() -> {
                printer.add("StarCentre: ").add(joinPiece.getCentre().getAuxState().getId()).newLine();
                printer.add("StarCorners:").newLine();
                printer.indentEnclose(() -> {
                    for (int i = 0; i < joinPiece.getCorners().size(); ++i) {
                        StarJoinPiece.StarCorner corner = joinPiece.getCorners().get(i);
                        printer.add("Corner#").add(i).add(": ").add(corner.getPiece().getAuxState().getId()).newLine();
                        printer.indentEnclose(() -> {
                            printer.add("JoinType: ").add(corner.getJoinType().toSql()).newLine();
                            printer.add("EqualJoinConditions:").newLine();
                            printer.indentEnclose(() -> {
                                corner.getEqConjuncts().forEach(printer::add);
                                printer.newLine();
                            });
                            printer.add("OtherJoinConditions:").newLine();
                            printer.indentEnclose(() -> {
                                corner.getOtherConjuncts().forEach(printer::add);
                                printer.newLine();
                            });
                        });
                    }
                });
            });
            joinPiece.getInputPieces().forEach(input -> context.addIndent(0));
            return visit(joinPiece, context);
        }

        private PrettyPrinter print(PlanPiece planPiece, PrettyPrinter prettyPrinter, int level) {
            planPiece.assignPieceIds();
            Context context = new Context(Optional.ofNullable(prettyPrinter).orElseGet(PrettyPrinter::new));
            context.addIndent(0);
            printImpl(planPiece, context, level);
            return context.getPrinter();
        }

        private void printImpl(PlanPiece planPiece, Context context, int level) {
            if (level <= 0) {
                return;
            }
            final int numIndents = context.indent();
            IntStream.range(0, numIndents).forEach(i -> context.getPrinter().pushIndent());
            planPiece.accept(this, context);
            planPiece.getInputPieces().forEach(input -> printImpl(input, context, level - 1));
            IntStream.range(0, numIndents).forEach(i -> context.getPrinter().popIndent());
        }
    }
}
