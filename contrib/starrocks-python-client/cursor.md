# Cursor AI Guide for starrocks-python-client

This document provides context and guidance for the AI assistant (Cursor) when collaborating on the `starrocks-python-client` project.

## 1. Project Overview

- **Project Name**: `starrocks-python-client`
- **Core Goal**: To provide a full-featured Python client, especially by implementing a powerful SQLAlchemy Dialect and Alembic integration, enabling Python developers to seamlessly use StarRocks' advanced features.
- **Technology Stack**:
  - **Language**: Python
  - **Core Libraries**: SQLAlchemy, Alembic
  - **Testing**: pytest
- **Key Modules**:
  - `starrocks/dialect.py`: Core implementation of the SQLAlchemy Dialect, responsible for SQL dialect translation, connection management, and execution.
  - `starrocks/reflection.py`: Responsible for database object "reflection," i.e., reading metadata of tables, views, indexes, etc., from a StarRocks database.
  - `starrocks/alembic/`: Specific implementation for Alembic integration, used for handling the autogeneration and execution of database migration scripts.
  - `test/`: Directory for unit and integration tests.

## 2. Current Development Goal

Our current primary goal is to **enhance support for StarRocks-specific features in SQLAlchemy and Alembic**. This includes, but is not limited to:

- **Improve Data Type Support**:
  - Add support for StarRocks-specific data types such as `BITMAP`, `HLL`, and `JSON`.
  - Ensure these types are correctly identified and handled in SQLAlchemy model definitions and Alembic migration scripts.
- **Enhance DDL Support**:
  - Implement full support for StarRocks table attributes (e.g., `engine`, `distribution`, `order by`, `properties`).
  - Enable the Alembic `autogenerate` process to accurately detect and generate change scripts for these attributes.
- **Views and Materialized Views**:
  - Add support for creating, dropping, and reflecting StarRocks Views and Materialized Views.
  - Ensure Alembic can manage view migrations.
- **Improve Compatibility**:
  - Fix bugs and compatibility issues encountered when SQLAlchemy or Alembic interact with StarRocks.

## 3. Coding & Development Workflow

Please follow these standards and procedures when assisting with development:

- **Design First**: Before any coding, please provide a design outline that includes:

  1. **Implementation Approach**: Describe how you will implement the feature.
  2. **Code Change Points**: Identify which files and functions need modification.
  3. **Test Cases**: List the test cases that will be added or modified to verify the functionality.
     [[memory:8065643]]

- **Clarity and Conciseness**: For any functional design or modification, a concise and clear design document is required before implementation begins.

- **Coding Standards**:
  - **Python Code Style**: Follow the python code style, maybe in the [code_style.md](code_style.md).
  - **Type Annotations**: All functions, methods, and complex variable declarations should have explicit type annotations. Historical code should also be gradually annotated. [[memory:8065598]]
  - **Docstrings**: Write clear, Google-style docstrings for all modules, classes, and functions, including arguments and returns. In the docstrings of specific test classes (e.g., `TestAlterTableIntegration`), please follow this field order: `engine`, `key`, `comment`, `partition`, `distribution`, `order by`, `properties`. [[memory:8012332]]
  - **Code Modifications**: When refactoring or making changes, strictly limit them to the current task's scope and avoid altering unrelated code. [[memory:8012264]]
  - **Markdownlint**: Follow the project's Markdownlint standards for all Markdown files.

- **Testing Requirements**:
  - **Framework**: Use `pytest`.
  - **Practice**:
    - Every new feature or bug fix must have corresponding test cases, following a "unit tests first, then integration tests" sequence.
  - **Review**: I expect to review the test cases you write or modify before you implement the code to pass them. [[memory:8065598]]
  - **Running Tests**: Use the `pytest test/` command to run all tests.

## 4. Documentation Standards

To ensure the project's design, testing strategies, and usage are well-documented and kept up-to-date, we will maintain a set of living documents under the `docs/` directory. This practice is crucial for developers, reviewers, and the AI assistant to have a shared understanding of the project.

The AI assistant is expected to help draft and update these documents as part of the development workflow.

- **Key Documents**:

  - `docs/design/`: Contains the high-level architecture and detailed design of important modules, functions, and workflows.
  - `docs/test/`: Outlines the testing strategy, including test ideas, key test cases, and coverage goals.
  - `docs/usage_guide/`: Provides comprehensive usage guides and detailed API references for all user-facing features.

- **Documentation Workflow**:

  1. **Design Phase**: When proposing a new feature or a significant change, the design outline created under the "Design First" principle should be used to create or update the corresponding design document in `docs/design/`.
  2. **Testing Phase**: New test cases and testing strategies should be documented in `docs/test/` before or during implementation.
  3. **Implementation Phase**: Once an implementation is complete, the corresponding usage guide in `docs/usage_guide/` must be updated to reflect the changes and provide clear instructions for users.

- **How to Use This Information**:
  - **For Development**: Refer to `docs/design/` when implementing new features to ensure alignment with the intended architecture.
  - **For Testing**: Use `docs/design/` and `docs/test/` as references for writing comprehensive and relevant test cases.
  - **For Users**: `docs/usage_guide/` is the primary source of truth for understanding how to use the client.

## 5. Environment & Dependencies

- **Dependency Management**: Project dependencies are managed via `setup.py`.
- **Local Development**: A running StarRocks instance is typically required for integration testing. Connection information is usually configured via environment variables.
