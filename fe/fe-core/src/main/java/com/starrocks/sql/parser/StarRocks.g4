// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

grammar StarRocks;
import StarRocksLex;

sqlStatements
    : (singleStatement (SEMICOLON EOF? | EOF))+
    ;

singleStatement
    : statement
    ;

statement
    // Query Statement
    : queryStatement                                                                        #query

    // Table Statement
    | createTableStatement                                                                  #createTable
    | createTableAsSelectStatement                                                          #createTableAsSelect
    | alterTableStatement                                                                   #alterTable
    | dropTableStatement                                                                    #dropTable
    | showTableStatement                                                                    #showTables
    | showColumnStatement                                                                   #showColumn
    | showTableStatusStatement                                                              #showTableStatus
    | createIndexStatement                                                                  #createIndex
    | dropIndexStatement                                                                    #dropIndex
    | refreshTableStatement                                                                 #refreshTable

    // View Statement
    | createViewStatement                                                                   #createView
    | alterViewStatement                                                                    #alterView
    | dropViewStatement                                                                     #dropView

    // Task Statement
    | submitTaskStatement                                                                   #submitTask

    // Materialized View Statement
    | createMaterializedViewStatement                                                       #createMaterializedView
    | showMaterializedViewStatement                                                         #showMaterializedView
    | dropMaterializedViewStatement                                                         #dropMaterializedView

    // Catalog Statement
    | createExternalCatalogStatement                                                        #createCatalog
    | dropExternalCatalogStatement                                                          #dropCatalog
    | showCatalogsStatement                                                                 #showCatalogs

    // DML Statement
    | insertStatement                                                                       #insert
    | updateStatement                                                                       #update
    | deleteStatement                                                                       #delete

    // Admin Set Statement
    | ADMIN SET FRONTEND CONFIG '(' property ')'                                            #adminSetConfig
    | ADMIN SET REPLICA STATUS properties                                                   #adminSetReplicaStatus

    // Cluster Mangement Statement
    | alterSystemStatement                                                                  #alterSystem

    // Analyze Statement
    | analyzeStatement                                                                      #analyze
    | createAnalyzeStatement                                                                #createAnalyze
    | dropAnalyzeJobStatement                                                               #dropAnalyzeJob
    | showAnalyzeStatement                                                                  #showAnalyze

    // Work Group Statement
    | createWorkGroupStatement                                                              #createWorkGroup
    | dropWorkGroupStatement                                                                #dropWorkGroup
    | alterWorkGroupStatement                                                               #alterWorkGroup
    | showWorkGroupStatement                                                                #showWorkGroup

    // Other statement
    | USE qualifiedName                                                                     #use
    | showDatabasesStatement                                                                #showDatabases
    | showVariablesStatement                                                                #showVariables

    // privilege
    | GRANT identifierOrString TO user                                                      #grantRole
    | GRANT IMPERSONATE ON user TO ( user | ROLE identifierOrString )                       #grantImpersonate
    | REVOKE identifierOrString FROM user                                                   #revokeRole
    | REVOKE IMPERSONATE ON user FROM ( user | ROLE identifierOrString )                    #revokeImpersonate
    | EXECUTE AS user (WITH NO REVERT)?                                                     #executeAs
    | showAuthenticationStatement                                                           #showAuthentication
    ;

// ------------------------------------------- Table Statement ---------------------------------------------------------

createTableStatement
    : CREATE EXTERNAL? TABLE (IF NOT EXISTS)? qualifiedName
          '(' columnDesc (',' columnDesc)* (',' indexDesc)* ')'
          engineDesc?
          charsetDesc?
          keyDesc?
          comment?
          partitionDesc?
          distributionDesc?
          rollupDesc?
          properties?
          extProperties?
     ;

columnDesc
    : identifier type charsetName? KEY? aggDesc? (NULL | NOT NULL)? defaultDesc? comment?
    ;

charsetName
    : CHAR SET identifier
    | CHARSET identifier
    ;

defaultDesc
    : DEFAULT (string| NULL | CURRENT_TIMESTAMP)
    ;

indexDesc
    : INDEX indexName=identifier identifierList indexType? comment?
    ;

engineDesc
    : ENGINE EQ identifier
    ;

charsetDesc
    : DEFAULT? CHARSET EQ? identifierOrString
    ;


keyDesc
    : (AGGREGATE | UNIQUE | PRIMARY | DUPLICATE) KEY identifierList
    ;

aggDesc
    : SUM
    | MAX
    | MIN
    | REPLACE
    | HLL_UNION
    | BITMAP_UNION
    | PERCENTILE_UNION
    | REPLACE_IF_NOT_NULL
    ;

rollupDesc
    : ROLLUP '(' addRollupClause (',' addRollupClause)* ')'
    ;

addRollupClause
    : rollupName=identifier identifierList (dupKeys)? (fromRollup)? properties?
    ;

dupKeys
    : DUPLICATE KEY identifierList
    ;

fromRollup
    : FROM identifier
    ;

createTableAsSelectStatement
    : CREATE TABLE (IF NOT EXISTS)? qualifiedName
        ('(' identifier (',' identifier)* ')')? comment?
        partitionDesc?
        distributionDesc?
        properties?
        AS queryStatement
        ;

dropTableStatement
    : DROP TABLE (IF EXISTS)? qualifiedName FORCE?
    ;

alterTableStatement
    : ALTER TABLE qualifiedName alterClause (',' alterClause)*
    ;

createIndexStatement
    : CREATE INDEX indexName=identifier
        ON qualifiedName identifierList indexType?
        comment?
    ;

dropIndexStatement
    : DROP INDEX indexName=identifier ON qualifiedName
    ;

indexType
    : USING BITMAP
    ;

showTableStatement
    : SHOW FULL? TABLES ((FROM | IN) db=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    ;

showColumnStatement
    : SHOW FULL? COLUMNS ((FROM | IN) table=qualifiedName) ((FROM | IN) db=qualifiedName)?
        ((LIKE pattern=string) | (WHERE expression))?
    ;

showTableStatusStatement
    : SHOW TABLE STATUS ((FROM | IN) db=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    ;

refreshTableStatement
    : REFRESH EXTERNAL TABLE qualifiedName (PARTITION '(' string (',' string)* ')')?
    ;

// ------------------------------------------- View Statement ----------------------------------------------------------

createViewStatement
    : CREATE VIEW (IF NOT EXISTS)? qualifiedName
        ('(' columnNameWithComment (',' columnNameWithComment)* ')')?
        comment? AS queryStatement
    ;

alterViewStatement
    : ALTER VIEW qualifiedName
    ('(' columnNameWithComment (',' columnNameWithComment)* ')')?
    AS queryStatement
    ;

dropViewStatement
    : DROP VIEW (IF EXISTS)? qualifiedName
    ;

// ------------------------------------------- Task Statement ----------------------------------------------------------

submitTaskStatement
    : SUBMIT setVarHint* TASK qualifiedName?
    AS createTableAsSelectStatement
    ;

// ------------------------------------------- Materialized View Statement ---------------------------------------------

createMaterializedViewStatement
    : CREATE MATERIALIZED VIEW (IF NOT EXISTS)? mvName=qualifiedName
    comment?
    (PARTITION BY primaryExpression)?
    distributionDesc?
    refreshSchemeDesc?
    properties?
    AS queryStatement
    ;

showMaterializedViewStatement
    : SHOW MATERIALIZED VIEW ((FROM | IN) db=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    ;

dropMaterializedViewStatement
    : DROP MATERIALIZED VIEW (IF EXISTS)? mvName=qualifiedName
    ;

// ------------------------------------------- Cluster Mangement Statement ---------------------------------------------

alterSystemStatement
    : ALTER SYSTEM alterClause
    ;

// ------------------------------------------- Catalog Statement -------------------------------------------------------

createExternalCatalogStatement
    : CREATE EXTERNAL CATALOG catalogName=identifierOrString comment? properties
    ;

dropExternalCatalogStatement
    : DROP CATALOG catalogName=identifierOrString
    ;

showCatalogsStatement
    : SHOW CATALOGS
    ;


// ------------------------------------------- Alter Clause ------------------------------------------------------------

alterClause
    : createIndexClause
    | dropIndexClause
    | tableRenameClause

    | addBackendClause
    | dropBackendClause
    | addFrontendClause
    | dropFrontendClause
    ;

createIndexClause
    : ADD INDEX indexName=identifier identifierList indexType? comment?
    ;

dropIndexClause
    : DROP INDEX indexName=identifier
    ;

tableRenameClause
    : RENAME identifier
    ;

addBackendClause
   : ADD FREE? BACKEND (TO identifier)? string (',' string)*
   ;

dropBackendClause
   : DROP BACKEND string (',' string)* FORCE?
   ;

addFrontendClause
   : ADD (FOLLOWER | OBSERVER) string
   ;

dropFrontendClause
   : DROP (FOLLOWER | OBSERVER) string
   ;

// ------------------------------------------- DML Statement -----------------------------------------------------------

insertStatement
    : explainDesc? INSERT INTO qualifiedName partitionNames?
        (WITH LABEL label=identifier)? columnAliases?
        (queryStatement | (VALUES expressionsWithDefault (',' expressionsWithDefault)*))
    ;

updateStatement
    : explainDesc? UPDATE qualifiedName SET assignmentList (WHERE where=expression)?
    ;

deleteStatement
    : explainDesc? DELETE FROM qualifiedName partitionNames? (WHERE where=expression)?
    ;

// ------------------------------------------- Analyze Statement -------------------------------------------------------

analyzeStatement
    : ANALYZE FULL? TABLE qualifiedName ('(' identifier (',' identifier)* ')')? properties?
    ;

createAnalyzeStatement
    : CREATE ANALYZE FULL? ALL properties?
    | CREATE ANALYZE FULL? DATABASE db=identifier properties?
    | CREATE ANALYZE FULL? TABLE qualifiedName ('(' identifier (',' identifier)* ')')? properties?
    ;

dropAnalyzeJobStatement
    : DROP ANALYZE INTEGER_VALUE
    ;

showAnalyzeStatement
    : SHOW ANALYZE
    ;

// ------------------------------------------- Work Group Statement ----------------------------------------------------

createWorkGroupStatement
    : CREATE RESOURCE GROUP (IF NOT EXISTS)? (OR REPLACE)? identifier
        TO classifier (',' classifier)*  WITH '(' property (',' property)* ')'
    ;

dropWorkGroupStatement
    : DROP RESOURCE GROUP identifier
    ;

alterWorkGroupStatement
    : ALTER RESOURCE GROUP identifier ADD classifier (',' classifier)*
    | ALTER RESOURCE GROUP identifier DROP '(' INTEGER_VALUE (',' INTEGER_VALUE)* ')'
    | ALTER RESOURCE GROUP identifier DROP ALL
    | ALTER RESOURCE GROUP identifier WITH '(' property (',' property)* ')'
    ;

showWorkGroupStatement
    : SHOW RESOURCE GROUP identifier
    | SHOW RESOURCE GROUPS ALL?
    ;

classifier
    : '(' expression (',' expression)* ')'
    ;

// ------------------------------------------- Other Statement ---------------------------------------------------------

showDatabasesStatement
    : SHOW DATABASES ((FROM | IN) catalog=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    | SHOW SCHEMAS ((LIKE pattern=string) | (WHERE expression))?
    ;

showVariablesStatement
    : SHOW varType? VARIABLES ((LIKE pattern=string) | (WHERE expression))?
    ;

varType
    : GLOBAL
    | LOCAL
    | SESSION
    ;

showAuthenticationStatement
    : SHOW ALL AUTHENTICATION                                   #showAllAuthentication
    | SHOW AUTHENTICATION (FOR user)?                           #showAuthenticationForUser
    ;

// ------------------------------------------- Query Statement ---------------------------------------------------------

queryStatement
    : explainDesc? queryBody outfile?;

queryBody
    : withClause? queryNoWith
    ;

withClause
    : WITH commonTableExpression (',' commonTableExpression)*
    ;

queryNoWith
    :queryTerm (ORDER BY sortItem (',' sortItem)*)? (limitElement)?
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(UNION | EXCEPT | MINUS)
        setQuantifier? right=queryTerm                                         #setOperation
    ;

queryPrimary
    : querySpecification                           #queryPrimaryDefault
    | subquery                                     #subqueryPrimary
    ;

subquery
    : '(' queryBody  ')'
    ;

rowConstructor
     :'(' expression (',' expression)* ')'
     ;

sortItem
    : expression ordering = (ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

limitElement
    : LIMIT limit =INTEGER_VALUE (OFFSET offset=INTEGER_VALUE)?
    | LIMIT offset =INTEGER_VALUE ',' limit=INTEGER_VALUE
    ;

querySpecification
    : SELECT setVarHint* setQuantifier? selectItem (',' selectItem)*
      fromClause
      (WHERE where=expression)?
      (GROUP BY groupingElement)?
      (HAVING having=expression)?
    ;

fromClause
    : (FROM relations)?                                                                 #from
    | FROM DUAL                                                                         #dual
    ;

groupingElement
    : ROLLUP '(' (expression (',' expression)*)? ')'                                    #rollup
    | CUBE '(' (expression (',' expression)*)? ')'                                      #cube
    | GROUPING SETS '(' groupingSet (',' groupingSet)* ')'                              #multipleGroupingSets
    | expression (',' expression)*                                                      #singleGroupingSet
    ;

groupingSet
    : '(' expression? (',' expression)* ')'
    ;

commonTableExpression
    : name=identifier (columnAliases)? AS '(' queryBody ')'
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? (identifier | string))?                                            #selectSingle
    | qualifiedName '.' ASTERISK_SYMBOL                                                  #selectAll
    | ASTERISK_SYMBOL                                                                    #selectAll
    ;

relations
    : relation (',' LATERAL? relation)*
    ;

relation
    : relationPrimary joinRelation*
    | '(' relationPrimary joinRelation* ')'
    ;

relationPrimary
    : qualifiedName partitionNames? tabletList? (
        AS? alias=identifier columnAliases?)? bracketHint?                              #tableAtom
    | '(' VALUES rowConstructor (',' rowConstructor)* ')'
        (AS? alias=identifier columnAliases?)?                                          #inlineTable
    | subquery (AS? alias=identifier columnAliases?)?                                   #subqueryRelation
    | qualifiedName '(' expression (',' expression)* ')'
        (AS? alias=identifier columnAliases?)?                                          #tableFunction
    | '(' relations ')'                                                                 #parenthesizedRelation
    ;

joinRelation
    : crossOrInnerJoinType bracketHint?
            LATERAL? rightRelation=relationPrimary joinCriteria?
    | outerAndSemiJoinType bracketHint?
            LATERAL? rightRelation=relationPrimary joinCriteria
    ;

crossOrInnerJoinType
    : JOIN | INNER JOIN
    | CROSS | CROSS JOIN
    ;

outerAndSemiJoinType
    : LEFT JOIN | RIGHT JOIN | FULL JOIN
    | LEFT OUTER JOIN | RIGHT OUTER JOIN
    | FULL OUTER JOIN
    | LEFT SEMI JOIN | RIGHT SEMI JOIN
    | LEFT ANTI JOIN | RIGHT ANTI JOIN
    ;

bracketHint
    : '[' IDENTIFIER (',' IDENTIFIER)* ']'
    ;

setVarHint
    : '/*+' SET_VAR '(' hintMap (',' hintMap)* ')' '*/'
    ;

hintMap
    : k=IDENTIFIER '=' v=literalExpression
    ;

joinCriteria
    : ON expression
    | USING '(' identifier (',' identifier)* ')'
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

partitionNames
    : TEMPORARY? (PARTITION | PARTITIONS) '(' identifier (',' identifier)* ')'
    | TEMPORARY? (PARTITION | PARTITIONS) identifier
    ;

tabletList
    : TABLET '(' INTEGER_VALUE (',' INTEGER_VALUE)* ')'
    ;

// ------------------------------------------- Expression --------------------------------------------------------------

/**
 * Operator precedences are shown in the following list, from highest precedence to the lowest.
 *
 * !
 * - (unary minus), ~ (unary bit inversion)
 * ^
 * *, /, DIV, %, MOD
 * -, +
 * &
 * |
 * = (comparison), <=>, >=, >, <=, <, <>, !=, IS, LIKE, REGEXP
 * BETWEEN, CASE WHEN
 * NOT
 * AND, &&
 * XOR
 * OR, ||
 * = (assignment)
 */

expressionsWithDefault
    : '(' expressionOrDefault (',' expressionOrDefault)* ')'
    ;

expressionOrDefault
    : expression | DEFAULT
    ;

expression
    : booleanExpression                                                                   #expressionDefault
    | NOT expression                                                                      #logicalNot
    | left=expression operator=(AND|LOGICAL_AND) right=expression                         #logicalBinary
    | left=expression operator=(OR|LOGICAL_OR) right=expression                           #logicalBinary
    ;

booleanExpression
    : predicate                                                                           #booleanExpressionDefault
    | booleanExpression IS NOT? NULL                                                      #isNull
    | left = booleanExpression comparisonOperator right = predicate                       #comparison
    | booleanExpression comparisonOperator '(' queryBody ')'                              #scalarSubquery
    ;

predicate
    : valueExpression (predicateOperations[$valueExpression.ctx])?
    ;

predicateOperations [ParserRuleContext value]
    : NOT? IN '(' expression (',' expression)* ')'                                        #inList
    | NOT? IN '(' queryBody ')'                                                           #inSubquery
    | NOT? BETWEEN lower = valueExpression AND upper = predicate                          #between
    | NOT? (LIKE | RLIKE | REGEXP) pattern=valueExpression                                #like
    ;

valueExpression
    : primaryExpression                                                                   #valueExpressionDefault
    | left = valueExpression operator = BITXOR right = valueExpression                    #arithmeticBinary
    | left = valueExpression operator = (
              ASTERISK_SYMBOL
            | SLASH_SYMBOL
            | PERCENT_SYMBOL
            | INT_DIV
            | MOD)
      right = valueExpression                                                             #arithmeticBinary
    | left = valueExpression operator = (PLUS_SYMBOL | MINUS_SYMBOL)
        right = valueExpression                                                           #arithmeticBinary
    | left = valueExpression operator = BITAND right = valueExpression                    #arithmeticBinary
    | left = valueExpression operator = BITOR right = valueExpression                     #arithmeticBinary
    ;

primaryExpression
    : variable                                                                            #var
    | columnReference                                                                     #columnRef
    | functionCall                                                                        #functionCallExpression
    | '{' FN functionCall '}'                                                             #odbcFunctionCallExpression
    | primaryExpression COLLATE (identifier | string)                                     #collate
    | literalExpression                                                                   #literal
    | left = primaryExpression CONCAT right = primaryExpression                           #concat
    | operator = (MINUS_SYMBOL | PLUS_SYMBOL | BITNOT) primaryExpression                  #arithmeticUnary
    | operator = LOGICAL_NOT primaryExpression                                            #arithmeticUnary
    | '(' expression ')'                                                                  #parenthesizedExpression
    | EXISTS '(' queryBody ')'                                                            #exists
    | subquery                                                                            #subqueryExpression
    | CAST '(' expression AS type ')'                                                     #cast
    | CASE caseExpr=expression whenClause+ (ELSE elseExpression=expression)? END          #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | arrayType? '[' (expression (',' expression)*)? ']'                                  #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']'                               #arraySubscript
    | primaryExpression '[' start=INTEGER_VALUE? ':' end=INTEGER_VALUE? ']'               #arraySlice
    | primaryExpression ARROW string                                                      #arrowExpression
    ;

literalExpression
    : NULL                                                                                #nullLiteral
    | booleanValue                                                                        #booleanLiteral
    | number                                                                              #numericLiteral
    | (DATE | DATETIME) string                                                            #dateLiteral
    | string                                                                              #stringLiteral
    | interval                                                                            #intervalLiteral
    ;

functionCall
    : EXTRACT '(' identifier FROM valueExpression ')'                                     #extract
    | GROUPING '(' (expression (',' expression)*)? ')'                                    #groupingOperation
    | GROUPING_ID '(' (expression (',' expression)*)? ')'                                 #groupingOperation
    | informationFunctionExpression                                                       #informationFunction
    | specialFunctionExpression                                                           #specialFunction
    | aggregationFunction over?                                                           #aggregationFunctionCall
    | windowFunction over                                                                 #windowFunctionCall
    | qualifiedName '(' (expression (',' expression)*)? ')'  over?                        #simpleFunctionCall
    ;

aggregationFunction
    : AVG '(' DISTINCT? expression ')'
    | COUNT '(' ASTERISK_SYMBOL? ')'
    | COUNT '(' DISTINCT? (expression (',' expression)*)? ')'
    | MAX '(' DISTINCT? expression ')'
    | MIN '(' DISTINCT? expression ')'
    | SUM '(' DISTINCT? expression ')'
    ;

variable
    : AT AT ((GLOBAL | SESSION | LOCAL) '.')? identifier
    ;

columnReference
    : identifier
    | qualifiedName
    ;

informationFunctionExpression
    : name = DATABASE '(' ')'
    | name = SCHEMA '(' ')'
    | name = USER '(' ')'
    | name = CONNECTION_ID '(' ')'
    | name = CURRENT_USER '(' ')'
    ;

specialFunctionExpression
    : CHAR '(' expression ')'
    | CURRENT_TIMESTAMP '(' ')'
    | DAY '(' expression ')'
    | HOUR '(' expression ')'
    | IF '(' (expression (',' expression)*)? ')'
    | LEFT '(' expression ',' expression ')'
    | LIKE '(' expression ',' expression ')'
    | MINUTE '(' expression ')'
    | MOD '(' expression ',' expression ')'
    | MONTH '(' expression ')'
    | QUARTER '(' expression ')'
    | REGEXP '(' expression ',' expression ')'
    | REPLACE '(' (expression (',' expression)*)? ')'
    | RIGHT '(' expression ',' expression ')'
    | RLIKE '(' expression ',' expression ')'
    | SECOND '(' expression ')'
    | TIMESTAMPADD '(' unitIdentifier ',' expression ',' expression ')'
    | TIMESTAMPDIFF '(' unitIdentifier ',' expression ',' expression ')'
    //| WEEK '(' expression ')' TODO: Support week(expr) function
    | YEAR '(' expression ')'
    | PASSWORD '(' string ')'
    ;

windowFunction
    : name = ROW_NUMBER '(' ')'
    | name = RANK '(' ')'
    | name = DENSE_RANK '(' ')'
    | name = NTILE  '(' expression? ')'
    | name = LEAD  '(' (expression (',' expression)*)? ')'
    | name = LAG '(' (expression (',' expression)*)? ')'
    | name = FIRST_VALUE '(' (expression (',' expression)*)? ')'
    | name = LAST_VALUE '(' (expression (',' expression)*)? ')'
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

over
    : OVER '('
        (PARTITION BY partition+=expression (',' partition+=expression)*)?
        (ORDER BY sortItem (',' sortItem)*)?
        windowFrame?
      ')'
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame
    ;

// ------------------------------------------- COMMON AST --------------------------------------------------------------

explainDesc
    : EXPLAIN (LOGICAL | VERBOSE | COSTS)?
    ;

partitionDesc
    : PARTITION BY RANGE identifierList '(' (rangePartitionDesc (',' rangePartitionDesc)*)? ')'
    | PARTITION BY LIST identifierList '(' (listPartitionDesc (',' listPartitionDesc)*)? ')'
    ;

listPartitionDesc
    : singleItemListPartitionDesc
    | multiItemListPartitionDesc
    ;

singleItemListPartitionDesc
    : PARTITION (IF NOT EXISTS)? identifier VALUES IN stringList propertyList?
    ;

multiItemListPartitionDesc
    : PARTITION (IF NOT EXISTS)? identifier VALUES IN '(' stringList (',' stringList)* ')' propertyList?
    ;

stringList
    : '(' string (',' string)* ')'
    ;

rangePartitionDesc
    : singleRangePartition
    | multiRangePartition
    ;

singleRangePartition
    : PARTITION (IF NOT EXISTS)? identifier VALUES partitionKeyDesc propertyList?
    ;

multiRangePartition
    : START '(' string ')' END '(' string ')' EVERY '(' interval ')'
    | START '(' string ')' END '(' string ')' EVERY '(' INTEGER_VALUE ')'
    ;

partitionKeyDesc
    : LESS THAN (MAXVALUE | partitionValueList)
    | '[' partitionValueList ',' partitionValueList ')'
    ;

partitionValueList
    : '(' partitionValue (',' partitionValue)* ')'
    ;

partitionValue
    : MAXVALUE | string
    ;

distributionDesc
    : DISTRIBUTED BY HASH identifierList (BUCKETS INTEGER_VALUE)?
    ;

refreshSchemeDesc
    : REFRESH (SYNC
    | ASYNC (START '(' string ')')? EVERY '(' interval ')'
    | MANUAL)
    ;

properties
    : PROPERTIES '(' property (',' property)* ')'
    ;

extProperties
    : BROKER properties
    ;

propertyList
    : '(' property (',' property)* ')'
    ;

property
    : key=string '=' value=string
    ;

comment
    : COMMENT string
    ;

columnNameWithComment
    : identifier comment?
    ;

outfile
    : INTO OUTFILE file=string fileFormat? properties?
    ;

fileFormat
    : FORMAT AS (identifier | string)
    ;

string
    : SINGLE_QUOTED_TEXT
    | DOUBLE_QUOTED_TEXT
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE | EQ_FOR_NULL
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL value=expression from=unitIdentifier
    ;

unitIdentifier
    : YEAR | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND | QUARTER
    ;

type
    : baseType
    | decimalType
    | arrayType
    ;

arrayType
    : ARRAY '<' type '>'
    ;

typeParameter
    : '(' INTEGER_VALUE ')'
    ;

baseType
    : BOOLEAN
    | TINYINT typeParameter?
    | SMALLINT typeParameter?
    | SIGNED INT?
    | INT typeParameter?
    | INTEGER typeParameter?
    | BIGINT typeParameter?
    | LARGEINT typeParameter?
    | FLOAT
    | DOUBLE
    | DATE
    | DATETIME
    | TIME
    | CHAR typeParameter?
    | VARCHAR typeParameter?
    | STRING
    | BITMAP
    | HLL
    | PERCENTILE
    | JSON
    ;

decimalType
    : (DECIMAL | DECIMALV2 | DECIMAL32 | DECIMAL64 | DECIMAL128) ('(' precision=INTEGER_VALUE (',' scale=INTEGER_VALUE)? ')')?
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

identifierList
    : '(' identifier (',' identifier)* ')'
    ;

identifierOrString
    : identifier
    | string
    ;

user
    : identifierOrString                                     # userWithoutHost
    | identifierOrString '@' identifierOrString              # userWithHost
    | identifierOrString '@' '[' identifierOrString ']'      # userWithHostAndBlanket
    ;

assignment
    : identifier EQ expressionOrDefault
    ;

assignmentList
    : assignment (',' assignment)*
    ;

number
    : DECIMAL_VALUE  #decimalValue
    | DOUBLE_VALUE   #doubleValue
    | INTEGER_VALUE  #integerValue
    ;

nonReserved
    : AFTER | AGGREGATE | ASYNC | AUTHORS | AVG | ADMIN
    | BACKEND | BACKENDS | BACKUP | BEGIN | BITMAP_UNION | BOOLEAN | BROKER | BUCKETS | BUILTIN
    | CAST | CATALOG | CATALOGS | CHAIN | CHARSET | CURRENT | COLLATION | COLUMNS | COMMENT | COMMIT | COMMITTED
    | CONNECTION | CONNECTION_ID | CONSISTENT | COSTS | COUNT | CONFIG
    | DATA | DATE | DATETIME | DAY | DISTRIBUTION | DUPLICATE | DYNAMIC
    | END | ENGINE | ENGINES | ERRORS | EVENTS | EXECUTE | EXTERNAL | EXTRACT | EVERY
    | FILE | FILTER | FIRST | FOLLOWING | FORMAT | FN | FRONTEND | FRONTENDS | FOLLOWER | FREE | FUNCTIONS
    | GLOBAL | GRANTS
    | HASH | HELP | HLL_UNION | HOUR
    | IDENTIFIED | IMPERSONATE | INDEXES | INSTALL | INTERMEDIATE | INTERVAL | ISOLATION
    | LABEL | LAST | LESS | LEVEL | LIST | LOCAL | LOGICAL
    | MANUAL | MATERIALIZED | MAX | MIN | MINUTE | MODIFY | MONTH | MERGE
    | NAME | NAMES | NEGATIVE | NO | NULLS
    | OBSERVER | OFFSET | ONLY | OPEN
    | PARTITIONS | PASSWORD | PATH | PAUSE | PERCENTILE_UNION | PLUGIN | PLUGINS | PRECEDING | PROC | PROCESSLIST
    | PROPERTIES | PROPERTY
    | QUARTER | QUERY | QUOTA
    | RANDOM | RECOVER | REFRESH | REPAIR | REPEATABLE | REPLACE_IF_NOT_NULL | REPLICA | REPOSITORY | REPOSITORIES
    | RESOURCE | RESTORE | RESUME | RETURNS | REVERT | ROLE | ROLES | ROLLUP | ROLLBACK | ROUTINE
    | SECOND | SERIALIZABLE | SESSION | SETS | SIGNED | SNAPSHOT | START | SUM | STATUS | STOP | STORAGE | STRING
    | SUBMIT | SYNC
    | TABLES | TABLET | TASK | TEMPORARY | TIMESTAMP | TIMESTAMPADD | TIMESTAMPDIFF | THAN | TIME | TRANSACTION
    | TRIGGERS | TRUNCATE | TYPE | TYPES
    | UNBOUNDED | UNCOMMITTED | UNINSTALL | USER
    | VALUE | VARIABLES | VIEW | VERBOSE
    | WARNINGS | WEEK | WORK | WRITE
    | YEAR
    ;
