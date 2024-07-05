package com.starrocks.analysis;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.parser.NodePosition;

import java.util.HashMap;
import java.util.Map;



public class ConvertIntervalExpr  extends Expr {

    public static final Map<String, TimestampArithmeticExpr.TimeUnit> SUPPORT_TYPE = new HashMap<String, TimestampArithmeticExpr.TimeUnit>();

    static {
       SUPPORT_TYPE.put(TimestampArithmeticExpr.TimeUnit.DAY.toString(), TimestampArithmeticExpr.TimeUnit.DAY);
        SUPPORT_TYPE.put(TimestampArithmeticExpr.TimeUnit.HOUR.toString(), TimestampArithmeticExpr.TimeUnit.HOUR);
        SUPPORT_TYPE.put(TimestampArithmeticExpr.TimeUnit.MINUTE.toString(), TimestampArithmeticExpr.TimeUnit.MINUTE);
        SUPPORT_TYPE.put(TimestampArithmeticExpr.TimeUnit.SECOND.toString(), TimestampArithmeticExpr.TimeUnit.SECOND);
        SUPPORT_TYPE.put(TimestampArithmeticExpr.TimeUnit.MILLISECOND.toString(), TimestampArithmeticExpr.TimeUnit.MILLISECOND);
        SUPPORT_TYPE.put(TimestampArithmeticExpr.TimeUnit.MICROSECOND.toString(), TimestampArithmeticExpr.TimeUnit.MICROSECOND);
    }


    private final String funcName;
    // Keep the original string passed in the c'tor to resolve
    // ambiguities with other uses of IDENT during query parsing.

    // Indicates an expr where the interval comes first, e.g., 'interval b year + a'.


    public ConvertIntervalExpr(String funcName, IntLiteral n, StringLiteral timeUnitIdent, StringLiteral toTimeUnitIdent, NodePosition pos) {
        super(pos);
        this.funcName = funcName;
        children.add(n);
        children.add(timeUnitIdent);
        children.add(toTimeUnitIdent);
    }

    public ConvertIntervalExpr(ConvertIntervalExpr convertIntervalExpr) {
        this(convertIntervalExpr.funcName, (IntLiteral) convertIntervalExpr.getChild(0), (StringLiteral) convertIntervalExpr.getChild(1), (StringLiteral) convertIntervalExpr.getChild(2), convertIntervalExpr.pos);
    }

    @Override
    protected String toSqlImpl() {
        return funcName + "( interval " + children.get(0).toSqlImpl() +  " " + ((StringLiteral)children.get(1)).getStringValue().toLowerCase() + " , " + ((StringLiteral)children.get(2)).getStringValue().toLowerCase() + " )";
    }

    public String getFuncName() {
        return funcName;
    }


    @Override
    public Expr clone() {
        return new ConvertIntervalExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitConvertIntervalExpr(this, context);
    }
}
