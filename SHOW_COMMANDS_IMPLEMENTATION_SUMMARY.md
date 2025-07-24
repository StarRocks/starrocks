# StarRocks Issue #52446: Support ORDER BY/WHERE for all show commands

## Summary
This implementation adds comprehensive support for ORDER BY, WHERE, LIKE, and LIMIT clauses to all StarRocks SHOW commands, making them consistent and more user-friendly.

## Changes Made

### 1. Grammar Updates (StarRocks.g4)
Updated the ANTLR grammar file to add support for the following clauses to all show statements:
- `((LIKE pattern=string) | (WHERE expression))?` - LIKE and WHERE clause support
- `(ORDER BY sortItem (',' sortItem)*)?` - ORDER BY support  
- `(limitElement)?` - LIMIT support

**Updated show statements in grammar:**
- showDatabasesStatement
- showTableStatement  
- showTemporaryTablesStatement
- showAuthorStatement
- showBackendsStatement
- showBrokerStatement
- showCharsetStatement
- showCollationStatement
- showDeleteStatement
- showDynamicPartitionStatement
- showEventsStatement
- showEnginesStatement
- showFrontendsStatement
- showPluginsStatement
- showRepositoriesStatement
- showOpenTableStatement
- showPrivilegesStatement
- showProcedureStatement
- showProcStatement
- showProcesslistStatement
- showProfilelistStatement
- showRunningQueriesStatement
- showStatusStatement
- showTransactionStatement
- showTriggersStatement
- showUserPropertyStatement
- showVariablesStatement
- showWarningStatement
- showRolesStatement
- showGrantsStatement
- showUserStatement
- showAuthenticationStatement
- showComputeNodesStatement
- showResourceStatement
- showSqlBlackListStatement
- showWhiteListStatement
- showBackendBlackListStatement
- showComputeNodeBlackListStatement
- showDataCacheRulesStatement

### 2. Base ShowStmt Class Enhancements
Enhanced the base `ShowStmt` class with:
- Added `pattern` field for LIKE clause support
- Added constructor that accepts all common clauses
- Added getter/setter methods for pattern, orderByElements, and limitElement

### 3. AST Classes Updated
**Completed:**
- ShowBackendsStmt - Added full clause support
- ShowFrontendsStmt - Added full clause support

**Partially Completed:**
- ShowStmt base class - Enhanced with common functionality

### 4. AstBuilder Parser Updates  
**Completed:**
- visitShowBackendsStatement - Updated to parse all clauses
- visitShowFrontendsStatement - Updated to parse all clauses

## Implementation Status

### ✅ Completed
1. Grammar file updates for all show statements
2. Base ShowStmt class enhancements
3. Two example implementations (ShowBackendsStmt, ShowFrontendsStmt)
4. AstBuilder updates for the example implementations

### 🔄 In Progress / To Do
1. **Update remaining AST classes** - Need to update constructors for ~60+ show statement classes to accept the new parameters (pattern, where, orderByElements, limitElement)

2. **Update remaining AstBuilder methods** - Need to update ~60+ visitShow*Statement methods to parse the new clauses

3. **Update ShowExecutor** - May need updates to handle the new clauses in query execution

4. **Update Analyzers** - May need updates in ShowStmtAnalyzer and related classes

5. **Add tests** - Create comprehensive tests for the new functionality

## Implementation Pattern

For each show statement, the pattern is:

### AST Class Pattern:
```java
public class ShowXxxStmt extends ShowStmt {
    private final String pattern;

    // Old constructor for backward compatibility
    public ShowXxxStmt(NodePosition pos) {
        this(null, null, null, null, pos);
    }

    // New constructor with full clause support
    public ShowXxxStmt(String pattern, Expr where, List<OrderByElement> orderByElements,
                       LimitElement limitElement, NodePosition pos) {
        super(pos);
        this.pattern = pattern;
        this.predicate = where;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
    }

    public String getPattern() {
        return pattern;
    }
}
```

### AstBuilder Method Pattern:
```java
@Override
public ParseNode visitShowXxxStatement(StarRocksParser.ShowXxxStatementContext context) {
    String pattern = null;
    Expr where = null;
    if (context.string() != null) {
        pattern = ((StringLiteral) visit(context.string())).getStringValue();
    }
    if (context.expression() != null) {
        where = (Expr) visit(context.expression());
    }
    List<OrderByElement> orderByElements = null;
    if (context.ORDER() != null) {
        orderByElements = new ArrayList<>();
        orderByElements.addAll(visit(context.sortItem(), OrderByElement.class));
    }
    LimitElement limitElement = null;
    if (context.limitElement() != null) {
        limitElement = (LimitElement) visit(context.limitElement());
    }
    return new ShowXxxStmt(pattern, where, orderByElements, limitElement, createPos(context));
}
```

## Expected Benefits

1. **Consistency** - All show commands will support the same set of clauses
2. **User-friendliness** - Users can filter, sort, and limit results from any show command
3. **Backward compatibility** - Existing show commands continue to work unchanged
4. **Extensibility** - New show commands will naturally inherit the full clause support

## Next Steps

1. Continue updating the remaining AST classes using the established pattern
2. Update the corresponding AstBuilder methods  
3. Test the implementation with various show commands
4. Add comprehensive unit and integration tests
5. Update documentation to reflect the new capabilities

## Files Modified

- `fe/fe-core/src/main/java/com/starrocks/sql/parser/StarRocks.g4` - Grammar updates
- `fe/fe-core/src/main/java/com/starrocks/sql/ast/ShowStmt.java` - Base class enhancements
- `fe/fe-core/src/main/java/com/starrocks/sql/ast/ShowBackendsStmt.java` - Example implementation
- `fe/fe-core/src/main/java/com/starrocks/sql/ast/ShowFrontendsStmt.java` - Example implementation  
- `fe/fe-core/src/main/java/com/starrocks/sql/parser/AstBuilder.java` - Parser updates

## Testing Commands

Once fully implemented, users will be able to run commands like:
```sql
SHOW BACKENDS WHERE Host LIKE '%server%' ORDER BY Host LIMIT 10;
SHOW TABLES WHERE Table_type = 'BASE TABLE' ORDER BY Name;
SHOW DATABASES LIKE 'test%' ORDER BY Database DESC;
```