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

package com.starrocks.sql.ast.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.TypeSerializer;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TAggregateExpr;
import com.starrocks.thrift.TBinaryLiteral;
import com.starrocks.thrift.TBoolLiteral;
import com.starrocks.thrift.TCaseExpr;
import com.starrocks.thrift.TDateLiteral;
import com.starrocks.thrift.TDecimalLiteral;
import com.starrocks.thrift.TDictionaryGetExpr;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TFloatLiteral;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TInPredicate;
import com.starrocks.thrift.TInfoFunc;
import com.starrocks.thrift.TIntLiteral;
import com.starrocks.thrift.TLargeIntLiteral;
import com.starrocks.thrift.TPlaceHolder;
import com.starrocks.thrift.TSlotRef;
import com.starrocks.thrift.TStringLiteral;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Convert {@link Expr} nodes into their Thrift representation via {@link AstVisitorExtendInterface}.
 */
public class ExprToThriftVisitor implements AstVisitorExtendInterface<Void, TExprNode> {

    public static final ExprToThriftVisitor INSTANCE = new ExprToThriftVisitor();

    protected ExprToThriftVisitor() {
    }

    // Convert this expr, including all children, to its Thrift representation.
    public static TExpr treeToThrift(Expr expr) {
        TExpr result = new TExpr();
        ExprToThriftVisitor.treeToThriftHelper(expr, result, ExprToThriftVisitor.INSTANCE::visit);
        return result;
    }

    public static List<TExpr> treesToThrift(List<? extends Expr> exprs) {
        List<TExpr> result = Lists.newArrayList();
        for (Expr expr : exprs) {
            result.add(ExprToThriftVisitor.treeToThrift(expr));
        }
        return result;
    }

    public static void treeToThriftHelper(Expr expr, TExpr container, BiConsumer<Expr, TExprNode> consumer) {
        if (expr.getType().isNull()) {
            Preconditions.checkState(expr instanceof NullLiteral || expr instanceof SlotRef);
            treeToThriftHelper(NullLiteral.create(ScalarType.BOOLEAN), container, consumer);
            return;
        }

        TExprNode msg = new TExprNode();

        Preconditions.checkState(java.util.Objects.equals(expr.type,
                AnalyzerUtils.replaceNullType2Boolean(expr.type)),
                "NULL_TYPE is illegal in thrift stage");

        msg.type = TypeSerializer.toThrift(expr.type);
        List<Expr> children = expr.getChildren();
        msg.num_children = children.size();
        msg.setHas_nullable_child(expr.hasNullableChild());
        msg.setIs_nullable(expr.isNullable());
        if (expr.fn != null) {
            TFunction tfn = expr.fn.toThrift();
            tfn.setIgnore_nulls(expr.getIgnoreNulls());
            msg.setFn(tfn);
            if (expr.fn.hasVarArgs()) {
                msg.setVararg_start_idx(expr.fn.getNumArgs() - 1);
            }
        }
        msg.output_scale = expr.getOutputScale();
        msg.setIs_monotonic(expr.isMonotonic());
        msg.setIs_index_only_filter(expr.isIndexOnlyFilter());
        consumer.accept(expr, msg);
        container.addToNodes(msg);
        for (Expr child : children) {
            treeToThriftHelper(child, container, consumer);
        }
    }

    @Override
    public Void visitExpression(Expr node, TExprNode msg) {
        throw new StarRocksPlannerException("Not implement toThrift function", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Void visitBetweenPredicate(BetweenPredicate node, TExprNode msg) {
        throw new IllegalStateException(
                "BetweenPredicate needs to be rewritten into a CompoundPredicate.");
    }

    @Override
    public Void visitArrayExpr(ArrayExpr node, TExprNode msg) {
        msg.setNode_type(TExprNodeType.ARRAY_EXPR);
        return null;
    }

    @Override
    public Void visitCollectionElementExpr(CollectionElementExpr node, TExprNode msg) {
        if (node.getChild(0).getType().isArrayType()) {
            msg.setNode_type(TExprNodeType.ARRAY_ELEMENT_EXPR);
        } else {
            msg.setNode_type(TExprNodeType.MAP_ELEMENT_EXPR);
        }
        msg.setCheck_is_out_of_bounds(node.isCheckIsOutOfBounds());
        return null;
    }

    @Override
    public Void visitArrowExpr(ArrowExpr node, TExprNode msg) {
        throw new StarRocksPlannerException("not support", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Void visitStringLiteral(StringLiteral node, TExprNode msg) {
        msg.node_type = TExprNodeType.STRING_LITERAL;
        msg.string_literal = new TStringLiteral(node.getUnescapedValue());
        return null;
    }

    @Override
    public Void visitArithmeticExpr(ArithmeticExpr node, TExprNode msg) {
        msg.node_type = TExprNodeType.ARITHMETIC_EXPR;
        msg.setOpcode(node.getOp().getOpcode());
        msg.setOutput_column(node.getOutputColumn());
        return null;
    }

    @Override
    public Void visitDictQueryExpr(DictQueryExpr node, TExprNode msg) {
        msg.setNode_type(TExprNodeType.DICT_QUERY_EXPR);
        msg.setDict_query_expr(node.getDictQueryExpr());
        return null;
    }

    @Override
    public Void visitLikePredicate(LikePredicate node, TExprNode msg) {
        msg.node_type = TExprNodeType.FUNCTION_CALL;
        return null;
    }

    @Override
    public Void visitLambdaArguments(LambdaArgument node, TExprNode msg) {
        throw new StarRocksPlannerException("not support", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Void visitInformationFunction(InformationFunction node, TExprNode msg) {
        msg.node_type = TExprNodeType.INFO_FUNC;
        msg.info_func = new TInfoFunc(node.getIntValue(), node.getStrValue());
        return null;
    }

    @Override
    public Void visitSubqueryExpr(Subquery node, TExprNode msg) {
        return null;
    }

    @Override
    public Void visitArraySliceExpr(ArraySliceExpr node, TExprNode msg) {
        msg.setNode_type(TExprNodeType.ARRAY_SLICE_EXPR);
        return null;
    }

    @Override
    public Void visitSlot(SlotRef node, TExprNode msg) {
        msg.node_type = TExprNodeType.SLOT_REF;
        SlotDescriptor desc = node.getDesc();
        if (desc != null) {
            if (desc.getParent() != null) {
                msg.slot_ref = new TSlotRef(desc.getId().asInt(), desc.getParent().getId().asInt());
            } else {
                msg.slot_ref = new TSlotRef(desc.getId().asInt(), 0);
            }
        } else {
            msg.slot_ref = new TSlotRef(0, 0);
        }
        msg.setOutput_column(node.getOutputColumn());
        return null;
    }

    @Override
    public Void visitDecimalLiteral(DecimalLiteral node, TExprNode msg) {
        msg.setNode_type(TExprNodeType.DECIMAL_LITERAL);
        TDecimalLiteral decimalLiteral = new TDecimalLiteral();
        decimalLiteral.setValue(node.getValue().toPlainString());
        decimalLiteral.setInteger_value(node.packDecimal());
        msg.setDecimal_literal(decimalLiteral);
        return null;
    }

    @Override
    public Void visitCaseWhenExpr(CaseExpr node, TExprNode msg) {
        msg.node_type = TExprNodeType.CASE_EXPR;
        msg.case_expr = new TCaseExpr(node.hasCaseExpr(), node.hasElseExpr());
        msg.setChild_type(node.getChild(0).getType().getPrimitiveType().toThrift());
        return null;
    }

    @Override
    public Void visitDictionaryGetExpr(DictionaryGetExpr node, TExprNode msg) {
        TDictionaryGetExpr dictionaryGetExpr = new TDictionaryGetExpr();
        dictionaryGetExpr.setDict_id(node.getDictionaryId());
        dictionaryGetExpr.setTxn_id(node.getDictionaryTxnId());
        dictionaryGetExpr.setKey_size(node.getKeySize());
        dictionaryGetExpr.setNull_if_not_exist(node.getNullIfNotExist());
        node.setDictionaryGetExpr(dictionaryGetExpr);

        msg.setNode_type(TExprNodeType.DICTIONARY_GET_EXPR);
        msg.setDictionary_get_expr(dictionaryGetExpr);
        return null;
    }

    @Override
    public Void visitVarBinaryLiteral(VarBinaryLiteral node, TExprNode msg) {
        msg.node_type = TExprNodeType.BINARY_LITERAL;
        msg.binary_literal = new TBinaryLiteral(ByteBuffer.wrap(node.getValue()));
        return null;
    }

    @Override
    public Void visitFunctionCall(FunctionCallExpr node, TExprNode msg) {
        if (node.isAggregate() || node.isAnalyticFnCall()) {
            msg.node_type = TExprNodeType.AGG_EXPR;
            msg.setAgg_expr(new TAggregateExpr(node.isMergeAggFn()));
        } else {
            msg.node_type = TExprNodeType.FUNCTION_CALL;
        }
        return null;
    }

    @Override
    public Void visitIsNullPredicate(IsNullPredicate node, TExprNode msg) {
        msg.node_type = TExprNodeType.FUNCTION_CALL;
        return null;
    }

    @Override
    public Void visitMatchExpr(MatchExpr node, TExprNode msg) {
        msg.node_type = TExprNodeType.MATCH_EXPR;
        msg.setOpcode(node.getMatchOperator().getOpcode());
        return null;
    }

    @Override
    public Void visitLambdaFunctionExpr(LambdaFunctionExpr node, TExprNode msg) {
        msg.setNode_type(TExprNodeType.LAMBDA_FUNCTION_EXPR);
        msg.setOutput_column(node.getCommonSubOperatorNum());
        return null;
    }

    @Override
    public Void visitCompoundPredicate(CompoundPredicate node, TExprNode msg) {
        msg.node_type = TExprNodeType.COMPOUND_PRED;
        msg.setOpcode(node.getOp().toThrift());
        return null;
    }

    @Override
    public Void visitLargeIntLiteral(LargeIntLiteral node, TExprNode msg) {
        msg.node_type = TExprNodeType.LARGE_INT_LITERAL;
        msg.large_int_literal = new TLargeIntLiteral(node.getValue().toString());
        return null;
    }

    @Override
    public Void visitCastExpr(CastExpr node, TExprNode msg) {
        msg.node_type = TExprNodeType.CAST_EXPR;
        msg.setOpcode(node.getOpcode());
        msg.setOutput_column(node.getOutputColumn());
        if (node.getChild(0).getType().isComplexType()) {
            msg.setChild_type_desc(TypeSerializer.toThrift(node.getChild(0).getType()));
        } else {
            msg.setChild_type(node.getChild(0).getType().getPrimitiveType().toThrift());
        }
        return null;
    }

    @Override
    public Void visitSubfieldExpr(SubfieldExpr node, TExprNode msg) {
        msg.setNode_type(TExprNodeType.SUBFIELD_EXPR);
        msg.setUsed_subfield_names(node.getFieldNames());
        msg.setCopy_flag(node.isCopyFlag());
        return null;
    }

    @Override
    public Void visitMaxLiteral(MaxLiteral node, TExprNode msg) {
        return null;
    }

    @Override
    public Void visitCloneExpr(CloneExpr node, TExprNode msg) {
        msg.setNode_type(TExprNodeType.CLONE_EXPR);
        return null;
    }

    @Override
    public Void visitLargeInPredicate(LargeInPredicate node, TExprNode msg) {
        throw new UnsupportedOperationException(
                "LargeInPredicate cannot be serialized to Thrift. " +
                "It should be transformed to Left semi/anti join via LargeInPredicateToJoinRule.");
    }

    @Override
    public Void visitBinaryPredicate(BinaryPredicate node, TExprNode msg) {
        msg.node_type = TExprNodeType.BINARY_PRED;
        msg.setOpcode(node.getOpcode());
        msg.setVector_opcode(node.getVectorOpcode());
        if (node.getChild(0).getType().isComplexType()) {
            msg.setChild_type_desc(TypeSerializer.toThrift(node.getChild(0).getType()));
        } else {
            msg.setChild_type(node.getChild(0).getType().getPrimitiveType().toThrift());
        }
        return null;
    }

    @Override
    public Void visitIntLiteral(IntLiteral node, TExprNode msg) {
        msg.node_type = TExprNodeType.INT_LITERAL;
        msg.int_literal = new TIntLiteral(node.getValue());
        return null;
    }

    @Override
    public Void visitDateLiteral(DateLiteral node, TExprNode msg) {
        msg.node_type = TExprNodeType.DATE_LITERAL;
        msg.date_literal = new TDateLiteral(node.getStringValue());
        return null;
    }

    @Override
    public Void visitMapExpr(MapExpr node, TExprNode msg) {
        msg.setNode_type(TExprNodeType.MAP_EXPR);
        return null;
    }

    @Override
    public Void visitInPredicate(InPredicate node, TExprNode msg) {
        Preconditions.checkState(!node.contains(Subquery.class));
        msg.in_predicate = new TInPredicate(node.isNotIn());
        msg.node_type = TExprNodeType.IN_PRED;
        msg.setOpcode(node.getOpcode());
        msg.setVector_opcode(node.getVectorOpcode());
        if (node.getChild(0).getType().isComplexType()) {
            msg.setChild_type_desc(TypeSerializer.toThrift(node.getChild(0).getType()));
        } else {
            msg.setChild_type(node.getChild(0).getType().getPrimitiveType().toThrift());
        }
        return null;
    }

    @Override
    public Void visitFieldReference(FieldReference node, TExprNode msg) {
        throw new StarRocksPlannerException("FieldReference not implement toThrift", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Void visitAnalyticExpr(AnalyticExpr node, TExprNode msg) {
        return null;
    }

    @Override
    public Void visitTimestampArithmeticExpr(TimestampArithmeticExpr node, TExprNode msg) {
        msg.node_type = TExprNodeType.COMPUTE_FUNCTION_CALL;
        msg.setOpcode(node.getOpcode());
        return null;
    }

    @Override
    public Void visitBoolLiteral(BoolLiteral node, TExprNode msg) {
        msg.node_type = TExprNodeType.BOOL_LITERAL;
        msg.bool_literal = new TBoolLiteral(node.getValue());
        return null;
    }

    @Override
    public Void visitFloatLiteral(FloatLiteral node, TExprNode msg) {
        msg.node_type = TExprNodeType.FLOAT_LITERAL;
        msg.float_literal = new TFloatLiteral(node.getValue());
        return null;
    }

    @Override
    public Void visitExistsPredicate(ExistsPredicate node, TExprNode msg) {
        Preconditions.checkState(false);
        return null;
    }

    @Override
    public Void visitIntervalLiteral(IntervalLiteral node, TExprNode msg) {
        throw new StarRocksPlannerException("IntervalLiteral not implement toThrift", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Void visitDictMappingExpr(DictMappingExpr node, TExprNode msg) {
        msg.setNode_type(TExprNodeType.DICT_EXPR);
        return null;
    }

    @Override
    public Void visitNullLiteral(NullLiteral node, TExprNode msg) {
        msg.node_type = TExprNodeType.NULL_LITERAL;
        return null;
    }

    @Override
    public Void visitPlaceHolderExpr(PlaceHolderExpr node, TExprNode msg) {
        msg.setNode_type(TExprNodeType.PLACEHOLDER_EXPR);
        msg.setVslot_ref(new TPlaceHolder());
        msg.vslot_ref.setNullable(node.isNullable());
        msg.vslot_ref.setSlot_id(node.getSlotId());
        return null;
    }

    @Override
    public Void visitDefaultValueExpr(DefaultValueExpr node, TExprNode msg) {
        return null;
    }
}
