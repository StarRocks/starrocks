# StarRocks FE Grammar (ANTLR)

This directory stores the ANTLR grammar files (.g4) for StarRocks SQL. These grammars are the source of truth for SQL syntax and are used to generate the lexer and parser consumed by the FE SQL parser and AST layers.

## Purpose
- Define the StarRocks SQL syntax using ANTLR 4 grammars.
- Generate lexer/parser sources consumed by the FE parsing module and the AST definitions under `com.starrocks.sql.ast`.
- Keep the parsing stage focused on syntactic structure; delegate semantic checks to the analyzer.

## Relationship to other modules
- fe-grammar (.g4) -> generated lexer/parser -> FE SQL parser and AST (see `fe/fe-parser/` and `com.starrocks.sql.ast`).
- Analyzer/optimizer build on top of the parsed AST in `fe/fe-core`.

## Typical layout
- `*.g4` files:
  - Lexer grammar (tokens, keywords, literals, whitespace/comments).
  - Parser grammar (SQL statements and expressions).
- Optionally, shared fragments reused across rules.

## Guidelines
- Keep grammar unambiguous and deterministic; prefer explicit rules over backtracking.
- Do not embed semantic logic or state in the grammar; keep semantics in the analyzer.
- Separate lexer and parser concerns; avoid mixing token definitions with complex parser actions.
- Use predicates sparingly and document the rationale when needed.
- Maintain backward compatibility for widely used constructs; document behavior changes.
- Add comprehensive tests for new or changed syntax (positive, negative, and edge cases).

## Regenerating parser/lexer
- Use ANTLR 4.x to regenerate the lexer/parser sources from `.g4` files.
- Direct outputs to the FE parsing module as configured by the project.
- Avoid heavy project-wide builds; prefer targeted generation as per development docs.

## Testing
- Add/adjust SQL test cases under the project's SQL test framework.
- Include syntax-only tests and analyzer/optimizer coverage where applicable.

## Notes
- The AST is defined elsewhere; keep the grammar free of temporary semantic storage.
- Coordinate grammar changes with the FE parser/analyzer owners to ensure compatibility.

References:
- Parser and AST: `fe/fe-parser/`, `com/starrocks/sql/ast`
- Development docs: https://docs.starrocks.io/

