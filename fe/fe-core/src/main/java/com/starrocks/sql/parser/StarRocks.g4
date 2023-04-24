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
    : queryStatement                                                                    #statementDefault
    | EXPLAIN (LOGICAL | VERBOSE | COSTS)? queryStatement                               #explain
    | EXPLAIN? INSERT INTO qualifiedName partitionNames? (WITH LABEL lable=identifier)? columnAliases?
    (queryStatement | (VALUES expressionsWithDefault (',' expressionsWithDefault)*))    #insert
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName
        ('(' identifier (',' identifier)* ')')? comment?
        partitionDesc?
        distributionDesc?
        properties?
        AS queryStatement                                                               #createTableAsSelect
    | USE schema=identifier                                                             #use
    | SHOW FULL? TABLES ((FROM | IN) db=qualifiedName)?
        ((LIKE pattern=string) | (WHERE expression))?                                   #showTables
    | SHOW (DATABASES | SCHEMAS) ((LIKE pattern=string) | (WHERE expression))?          #showDatabases
    | CREATE VIEW (IF NOT EXISTS)? qualifiedName
            ('(' columnNameWithComment (',' columnNameWithComment)* ')')?
            comment? AS queryStatement                               #createView
    | ALTER VIEW qualifiedName
        ('(' columnNameWithComment (',' columnNameWithComment)* ')')?
        AS queryStatement                                                               #alterView
    ;

partitionDesc
    : PARTITION BY RANGE identifierList '(' rangePartitionDesc (',' rangePartitionDesc)* ')'
    ;

rangePartitionDesc
    : singleRangePartition
    | multiRangePartition
    ;

singleRangePartition
    : PARTITION identifier VALUES partitionKeyDesc
    ;

multiRangePartition
    : START '(' string ')' END '(' string ')' EVERY '(' interval ')'
    | START '(' string ')' END '(' string ')' EVERY '(' INTEGER_VALUE ')'
    ;

partitionKeyDesc
    : LESS THAN (MAXVALUE | partitionValueList)
    | '[' partitionValueList ',' partitionValueList ']'
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

properties
    : PROPERTIES '(' property (',' property)* ')'
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

queryStatement
    : query outfile?;

query
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
    : '(' query  ')'
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
    : SELECT hint* setQuantifier? selectItem (',' selectItem)*
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
    : name=identifier (columnAliases)? AS '(' query ')'
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

hint
    : '[' IDENTIFIER (',' IDENTIFIER)* ']'
    | '/*+' SET_VAR '(' hintMap (',' hintMap)* ')' '*/'
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
    ;

tabletList
    : TABLET '(' INTEGER_VALUE (',' INTEGER_VALUE)* ')'
    ;

expressionsWithDefault
    : '(' expressionOrDefault (',' expressionOrDefault)* ')'
    ;

expressionOrDefault
    : expression | DEFAULT
    ;


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
    | booleanExpression comparisonOperator '(' query ')'                                  #scalarSubquery
    ;

predicate
    : valueExpression (predicateOperations[$valueExpression.ctx])?
    ;

predicateOperations [ParserRuleContext value]
    : NOT? IN '(' expression (',' expression)* ')'                                        #inList
    | NOT? IN '(' query ')'                                                               #inSubquery
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
    | EXISTS '(' query ')'                                                                #exists
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
    | DAY '(' expression ')'
    | HOUR '(' expression ')'
    | IF '(' (expression (',' expression)*)? ')'
    | LEFT '(' expression ',' expression ')'
    | LIKE '(' expression ',' expression ')'
    | MINUTE '(' expression ')'
    | MOD '(' expression ',' expression ')'
    | MONTH '(' expression ')'
    | REGEXP '(' expression ',' expression ')'
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
    | name = LEAD  '(' (expression (',' expression)*)? ')'
    | name = LAG '(' (expression (',' expression)*)? ')'
    | name = FIRST_VALUE '(' (expression (',' expression)*)? ')'
    | name = LAST_VALUE '(' (expression (',' expression)*)? ')'
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
    : YEAR | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND
    ;

type
    : baseType
    | decimalType ('(' precision=INTEGER_VALUE (',' scale=INTEGER_VALUE)? ')')?
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
    | TINYINT
    | SMALLINT
    | INT
    | INTEGER
    | BIGINT
    | LARGEINT
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
    : DECIMAL | DECIMALV2 | DECIMAL32 | DECIMAL64 | DECIMAL128
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

number
    : DECIMAL_VALUE  #decimalValue
    | DOUBLE_VALUE   #doubleValue
    | INTEGER_VALUE  #integerValue
    ;

nonReserved
    : AVG | ANTI
    | BOOLEAN | BUCKETS
    | CAST | CONNECTION_ID| CURRENT | COMMENT | COSTS | COUNT
    | DATA | DATABASE | DATE | DATETIME | DAY | DISTRIBUTED
    | END | EXTRACT | EVERY
    | FILTER | FIRST | FOLLOWING | FORMAT | FN
    | GLOBAL
    | HASH | HOUR
    | INTERVAL
    | LABEL | LAST | LESS | LOCAL | LOGICAL
    | MAX | MIN | MINUTE | MONTH | MINUS
    | NONE | NULLS
    | OFFSET
    | PARTITIONS | PASSWORD | PRECEDING | PROPERTIES
    | RANK | ROLLUP | ROW
    | SECOND | SEMI | SESSION | SETS | START | SUM | STRING
    | TABLES | TABLET | TEMPORARY | TIMESTAMPADD | TIMESTAMPDIFF | THAN | TIME | TYPE
    | UNBOUNDED | USER
    | VIEW | VERBOSE
    | WEEK
    | YEAR
    ;