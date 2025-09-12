# StarRocks SQL Parser and AST

This module implements the StarRocks SQL parser and defines the Abstract Syntax Tree (AST) for the StarRocks SQL dialect. It converts raw SQL text into a typed AST consumed by the analyzer and optimizer layers.

## Responsibilities
- Tokenize and parse SQL text into a strongly-typed AST
- Provide accurate error diagnostics with source positions
- Define the full set of StarRocks SQL AST nodes (DDL/DML/DQL/admin/transaction/resource statements and expressions)
- Offer visitor utilities to traverse and transform AST nodes

## Where things live
- Parser entry points and helpers: this package (`com.starrocks.sql.parser`)
- AST node definitions: `com.starrocks.sql.ast`
- AST visitors: `com.starrocks.sql.ast` (visitor classes and utilities)
- Analyzer and optimizer: outside of this module (e.g., `com.starrocks.sql.analyzer`, `com.starrocks.sql.optimizer`)

Note: Exact class names and file layout may evolve; consult sources in the above packages.

## Parsing pipeline
1. Input SQL text is tokenized and parsed by the parser front-end.
2. A typed AST is built for statements and expressions.
3. The analyzer validates semantics and decorates the AST with metadata.
4. The optimizer generates physical plans from the analyzed AST.

The parser is responsible only for steps 1–2.

## AST conventions
- Nodes represent StarRocks SQL constructs (statements and expressions).
- Statement nodes (e.g., DQL, DML, DDL) and expression nodes are distinct families.
- Nodes carry source location information (line/column) where available.
- A visitor pattern is provided (e.g., `AstVisitor`) for traversal.
- Nodes aim to be immutable after construction; analysis does not belong in constructors.

## Error handling
- Syntax errors include clear messages with line/column and offending token when possible.
- The parser tries to surface the earliest, most relevant error.
- Keep grammar/extensions unambiguous to reduce cascading diagnostics.

## Typical usage
- Parse SQL text into AST nodes using the parser entry point in this package.
- Walk the AST via visitor utilities to inspect or transform nodes.
- Hand the AST to the analyzer for semantic checks and binding.

Pseudo-flow:
- create parser with session/config if needed
- parse the SQL string into a list of statements
- for each statement, either run visitors or pass to analyzer

## Extending the syntax
When adding new SQL syntax:
1. Update the parser to recognize the new construct and produce an AST node.
2. Add a new AST class (or extend existing ones) under `com.starrocks.sql.ast`.
3. Provide visitor hooks and `toSql`/printer support if applicable.
4. Update analyzer and optimizer to understand the new node.
5. Add tests (positive, negative, and edge cases).

Guidelines:
- Prefer minimal and orthogonal syntax extensions.
- Preserve backward compatibility; avoid ambiguous productions.
- Always include source position in new nodes if possible.
- Keep AST construction free of semantic logic.
- Do not add setter methods to AST nodes; do not use the AST as temporary storage between the Parser and Analyzer; keep the AST immutable.

## Performance notes
- Parser instances are not intended to be shared across threads.
- Avoid excessive backtracking/ambiguity in grammar to keep parse times predictable.
- Keep AST lightweight; defer heavy work to analyzer/planner.

## Testing
- Parser unit tests: FE test modules (Java) for AST shape and error messages.
- End-to-end SQL tests: `test/sql/` for correctness and compatibility.
- Add representative cases for new syntax, including failure diagnostics.

## Known dialect notes
StarRocks follows a SQL dialect broadly aligned with common analytics systems and MySQL-like constructs, with StarRocks-specific extensions (e.g., materialized views, lakehouse connectors, resource/session management).

## References
- Frontend overview: `fe/fe-core/src/main/java/com/starrocks/`
- AST: `com.starrocks.sql.ast`
- Analyzer: `com.starrocks.sql.analyzer`
- Optimizer: `com.starrocks.sql.optimizer`
- Project docs: https://docs.starrocks.io/
