# View and MaterializedView Architecture Design

## Overview

This design unifies the representation of View and MaterializedView using SQLAlchemy's `Table` class, distinguishing them by `info["table_kind"]` (TABLE/VIEW/MATERIALIZED_VIEW).

**Key Design Principles**:

- **Unified Representation**: All objects (Table/View/MV) are internally `Table` objects
- **Flexible Interface**: Support convenience classes (`View()`, `MaterializedView()`), direct `Table()` usage, and ORM style
- **Layered Storage**: `table.info` for general attributes, `table.comment` for standard, `table.dialect_options['starrocks']` for dialect-specific
- **SQLAlchemy Conventions**: Follow Table's parameter patterns and use `starrocks_` prefix for dialect options

**Key Benefits**:

- ✅ Leverage SQLAlchemy's Table infrastructure (columns, reflection, comparison)
- ✅ Support both SQL strings and Selectable objects (type-safe queries) for view/mv definition
- ✅ Seamless `Table.autoload_with=engine` integration
- ✅ Full Alembic autogenerate support
- ✅ Single database query per object (with caching)

## 1. Design Goals

To unify the representation of View and MaterializedView using the `Table` class, distinguishing them by `info["table_kind"]` as TABLE/VIEW/MATERIALIZED_VIEW.

### Core Design Principles

1. **Unified and Flexible User Interface**:

   - Users can use `View()` / `MaterializedView()` convenience classes.
   - Users can also directly define them using `Table()` + `info`.
   - They can be used just like regular `Table()` objects, with additional markers and attributes for View or MV when needed.

2. **Unified Internal Representation**:

   - Regardless of the creation method, internally they are all `Table` objects.
   - All attributes are stored in standard `Table` locations.
   - This allows various components to treat them as regular Tables, with extra logic for View/MV when necessary.

3. **Layered Attribute Storage**:
   - **General Attributes** → `table.info`: `table_kind`, `definition`
   - **Standard Attributes** → `Table` standard attributes: `comment` (via `table.comment`)
   - **Dialect-Specific Attributes** → `table.dialect_options['starrocks']`: `partition_by`, `distributed_by`, `order_by`, `refresh`, `properties`, view `security` passed via `starrocks_*` parameters.

## 2. Attribute Storage Scheme

### Complete Attribute Mapping Table

| Attribute Category       | Storage Location                                       | Applicable Objects | Parameter Passing Method              | Description                            |
| ------------------------ | ------------------------------------------------------ | ------------------ | ------------------------------------- | -------------------------------------- |
| **Object Type**          | `table.info['table_kind']`                             | ALL                | `info={'table_kind': 'VIEW'}`         | Core identifier (one of TABLE/VIEW/MV) |
| **Definition Statement** | `table.info['definition']`                             | VIEW, MV           | `info={'definition': 'SELECT ...'}`   | SELECT statement                       |
| **General Comment**      | `table.comment`                                        | ALL                | `comment='...'`                       | SQLAlchemy standard attribute          |
| **Partitioning**         | `table.dialect_options['starrocks']['partition_by']`   | TABLE, MV          | `starrocks_partition_by='...'`        | Passed via kwargs                      |
| **Distribution**         | `table.dialect_options['starrocks']['distributed_by']` | TABLE, MV          | `starrocks_distributed_by='...'`      | Passed via kwargs                      |
| **Ordering Key**         | `table.dialect_options['starrocks']['order_by']`       | TABLE, MV          | `starrocks_order_by='...'`            | Passed via kwargs                      |
| **Refresh**              | `table.dialect_options['starrocks']['refresh']`        | MV                 | `starrocks_refresh='IMMEDIATE ASYNC'` | Passed via kwargs                      |
| **Properties**           | `table.dialect_options['starrocks']['properties']`     | TABLE, MV          | `starrocks_properties={...}`          | Passed via kwargs                      |
| **Security Type**        | `table.dialect_options['starrocks']['security']`       | VIEW               | `starrocks_security='DEFINER'`        | Passed via kwargs                      |

### Why this Categorization?

- **`table.info`**: SQLAlchemy's native metadata dictionary, suitable for storing cross-dialect general information (e.g., `table_kind`, `definition`).
- **`table.comment`**: A standard SQLAlchemy attribute, supported by most databases, so the standard attribute is used directly.
- **`table.dialect_options['starrocks']`**: Passed via `starrocks_*` parameters, which `Table` automatically parses and stores.

**Specifics**:

1. **`info["table_kind"]` (representing the actual type)**:
   - Definition: A string stored in `table.info["table_kind"]`.
   - Purpose: Identifies the **actual type** of the current object.
   - Possible values: One of `"TABLE"` (Table), `"VIEW"` (View), `"MATERIALIZED_VIEW"` (Materialized View) (mutually exclusive).
   - Default value: If `None` or empty, defaults to `"TABLE"`.

## 3. API Signature

### Design Principles

View and MaterializedView follow **SQLAlchemy Table conventions**:

- Parameter order: `(name, metadata, *args, **kwargs)`
- `definition` as optional keyword argument (but required). Otherwise, it will be very complicated.
- All dialect-specific parameters use `starrocks_` prefix

### Signatures

```python
# View
View(
    name: str,
    metadata: MetaData,
    *args,  # Column objects (optional)
    definition: str | Selectable,  # Required
    schema: Optional[str] = None,
    comment: Optional[str] = None,
    columns: Optional[List[str | dict]] = None,  # Alternative column specification
    starrocks_security: Optional[str] = None,  # INVOKER or NONE (DEFINER not supported)
    **kwargs
)

# MaterializedView
MaterializedView(
    name: str,
    metadata: MetaData,
    *args,  # Column objects (optional)
    definition: str | Selectable,  # Required
    schema: Optional[str] = None,
    comment: Optional[str] = None,
    columns: Optional[List[str | dict]] = None,  # Same as View
    keep_existing: bool = False,  # Same as View
    extend_existing: bool = False,  # Same as View
    starrocks_partition_by: Optional[str] = None,
    starrocks_distributed_by: Optional[str] = None,
    starrocks_order_by: Optional[str] = None,
    starrocks_refresh: Optional[str] = None,  # "[IMMEDIATE|DEFERRED] {ASYNC|MANUAL}"
    starrocks_properties: Optional[Dict[str, str]] = None,
    **kwargs
)
```

**Column Specification:**

Views support three ways to define columns (names and optional comments):

1. **SQLAlchemy Column objects** via `*args`

   ```python
   View('v',
        metadata,
        Column('id', INTEGER),
        Column('name', VARCHAR()),
        definition='...')
   ```

2. **List of strings** via `columns` parameter (names only)

   ```python
   View('v', metadata, definition='...', columns=['id', 'name'])
   ```

3. **List of dicts** via `columns` parameter (names with optional comments)

   ```python
   View('v', metadata, definition='...',
        columns=[
            {'name': 'id', 'comment': 'ID'},
            {'name': 'name'}])
   ```

> **Note**: StarRocks VIEW columns only support name and comment. Types are auto-inferred from the SELECT statement.

### Usage Examples

```python
from sqlalchemy import MetaData, Column, select
from starrocks.schema import View, MaterializedView
from starrocks.datatype import INTEGER, VARCHAR

metadata = MetaData()

# Basic view
view = View('my_view', metadata, definition='SELECT * FROM users')

# View with columns (SQLAlchemy style)
view = View('user_view', metadata,
           Column('id', INTEGER),
           Column('name', VARCHAR(50), comment='User name'),
           definition='SELECT id, name FROM users')

# View with columns (simplified style 1: list of strings)
view = View('user_view', metadata,
           definition='SELECT id, name FROM users',
           columns=['user_id', 'user_name'])

# View with columns (simplified style 2: list of dicts)
view = View('detailed_view', metadata,
           definition='SELECT id, name FROM users',
           columns=[
               {'name': 'user_id', 'comment': 'User ID'},
               {'name': 'user_name', 'comment': 'User name'}
           ])

# View with security
view = View('secure_view', metadata,
           definition='SELECT * FROM sensitive_data',
           starrocks_security='INVOKER')

# View from Selectable
stmt = select(users.c.id, users.c.name)
view = View('user_view', metadata, definition=stmt)

# Materialized View (basic)
mv = MaterializedView('user_stats', metadata,
                     definition='SELECT user_id, COUNT(*) FROM orders GROUP BY user_id',
                     starrocks_partition_by='user_id',
                     starrocks_refresh='ASYNC')

# Materialized View (with all options)
mv = MaterializedView('order_mv', metadata,
                     Column('user_id', INTEGER),
                     Column('total', INTEGER, comment='Total orders'),
                     definition='SELECT user_id, COUNT(*) as total FROM orders GROUP BY user_id',
                     comment='User order statistics',
                     starrocks_partition_by='user_id',
                     starrocks_distributed_by='HASH(user_id) BUCKETS 10',
                     starrocks_order_by='user_id',
                     starrocks_refresh='IMMEDIATE ASYNC',
                     starrocks_properties={'replication_num': '3'})
```

**Column Support Details:**

| Feature          | Supported | Notes                            |
| ---------------- | --------- | -------------------------------- |
| Column name      | ✅        | Can rename SELECT result columns |
| Column comment   | ✅        | Optional description             |
| Column type      | ❌        | Auto-inferred from SELECT        |
| Column nullable  | ❌        | Auto-inferred from SELECT        |
| Other attributes | ❌        | Not supported by StarRocks       |

> **Note**: Both View and MaterializedView support columns in the same way.

For detailed implementation, see [view_columns_implementation.md](./view_columns_implementation.md).

## 4. Implementation Plans for Each Layer

### 4.1 Schema Layer (`schema.py`)

#### Approach A: `View`/`MaterializedView` Inherits from `Table` ⭐ **Adopted**

**Implementation Idea**: Let `Table`'s `__init__` automatically handle all parameters.

**Good Enhancement**: Support both `str` and SQLAlchemy `Selectable` objects for `definition` parameter.

```python
DefinitionType = Union[str, "Selectable"]

class View(Table):
    def __init__(
        self,
        name: str,
        definition: DefinitionType,  # Support both str and Selectable
        metadata: Optional[MetaData] = None,
        **kwargs
    ):
        """
        Create a View object.

        Args:
            definition: SQL string or SQLAlchemy Selectable object

        Examples:
            # String
            View('v1', 'SELECT * FROM users', metadata)

            # Selectable (type-safe, IDE support)
            stmt = select(users.c.id, users.c.name)
            View('v1', stmt, metadata)
        """
        # Set info
        info = kwargs.setdefault('info', {})
        info['table_kind'] = 'VIEW'

        # Handle both str and Selectable
        if isinstance(definition, str):
            info['definition'] = definition
        else:
            # Compile Selectable to SQL string
            from sqlalchemy.sql import ClauseElement
            if isinstance(definition, ClauseElement):
                compiled = definition.compile(compile_kwargs={"literal_binds": True})
                info['definition'] = str(compiled)
                info['_selectable'] = definition  # Keep original for reference
            else:
                raise TypeError(f"definition must be str or Selectable, got {type(definition)}")

        # Call Table.__init__, which automatically handles comment and starrocks_* parameters
        super().__init__(name, metadata, **kwargs)

    @property
    def definition(self) -> str:
        return self.info.get('definition', '')

    @property
    def selectable(self) -> Optional["Selectable"]:
        """Get original Selectable object if created from one"""
        return self.info.get('_selectable')

    @property
    def security(self):
        return self.dialect_options.get('starrocks', {}).get('security')

    # New: Column handling and existing table initialization
    def _process_definition(self, definition: DefinitionType) -> dict:
        """Process the definition parameter, supporting string and Selectable."""
        if isinstance(definition, str):
            return {'definition': definition}
        elif isinstance(definition, ClauseElement):
            compiled = definition.compile(compile_kwargs={"literal_binds": True})
            return {'definition': str(compiled), '_selectable': definition}
        else:
            raise TypeError(f"definition must be str or Selectable, got {type(definition)}")

    def _normalize_columns(self, columns: Optional[List[Union[Column, str, Dict]]] = None) -> List[Column]:
        """Normalize column definitions from various input formats."""
        normalized_cols = []
        if columns:
            for col in columns:
                if isinstance(col, Column):
                    normalized_cols.append(col)
                elif isinstance(col, str):
                    normalized_cols.append(Column(col, _always_force_type=True))
                elif isinstance(col, dict):
                    name = col.get("name")
                    comment = col.get("comment")
                    if not name:
                        raise ValueError("Column dictionary must contain a 'name' key.")
                    normalized_cols.append(Column(name, comment=comment, _always_force_type=True))
                else:
                    raise TypeError(f"Column must be Column object, string, or dict, got {type(col)}")
        return normalized_cols

    def _init_existing(self, *args, **kwargs):
        """Handle extend_existing=True case for View."""
        definition = kwargs.pop('definition', None)
        columns = kwargs.pop('columns', None)

        if definition is not None:
            view_info = self._process_definition(definition)
            self.info.update(view_info)

        if columns is not None:
            normalized_cols = self._normalize_columns(columns)
            # Clear existing columns and add new ones (this mimics how Table handles it)
            self.columns.clear()
            for col in normalized_cols:
                self.append_column(col)

        super()._init_existing(*args, **kwargs)


class MaterializedView(Table):
    def __init__(
        self,
        name: str,
        definition: DefinitionType,  # Same enhancement
        metadata: Optional[MetaData] = None,
        **kwargs
    ):
        info = kwargs.setdefault('info', {})
        info['table_kind'] = 'MATERIALIZED_VIEW'

        # Same handling as View
        if isinstance(definition, str):
            info['definition'] = definition
        else:
            from sqlalchemy.sql import ClauseElement
            if isinstance(definition, ClauseElement):
                compiled = definition.compile(compile_kwargs={"literal_binds": True})
                info['definition'] = str(compiled)
                info['_selectable'] = definition
            else:
                raise TypeError(f"definition must be str or Selectable, got {type(definition)}")

        super().__init__(name, metadata, **kwargs)

    @property
    def definition(self) -> str:
        return self.info.get('definition', '')

    @property
    def partition_by(self):
        return self.dialect_options.get('starrocks', {}).get('partition_by')

    @property
    def refresh(self):
        return self.dialect_options.get('starrocks', {}).get('refresh')

    # ... Other properties similar
```

#### MV `_init_existing` and Parameter Validation (Important)

MaterializedView should handle MV-specific parameters when `extend_existing=True`, and optionally validate inputs like `starrocks_refresh`.

```python
class MaterializedView(Table):
    def _init_existing(self, *args, **kwargs):
        """Handle MV-specific parameters during extend_existing."""
        refresh = kwargs.pop('starrocks_refresh', None)
        ...  # and other attributes

        opts = self.dialect_options.setdefault('starrocks', {})
        if refresh is not None:
            opts['refresh'] = refresh
        ...  # and other attributes

        # Delegate remaining updates (definition, columns, comment, etc.)
        super()._init_existing(*args, **kwargs)
```

**Pros**:

- Fully utilizes Table's parameter handling mechanism, resulting in concise code.
- **Supports both string and Selectable**: Flexible for different use cases (hand-written SQL vs type-safe queries).
- Users can mix positional arguments and `starrocks_*` arguments.
- Consistent with SQLAlchemy's design patterns.
- Reflection, Comparison, and other mechanisms are naturally supported.

**Cons**:

- Requires understanding Table's parameter handling mechanism.
- Parameter names need to be prefixed with `starrocks_` (but this is standard SQLAlchemy practice).

#### Critical: Understanding Table's `__new__` and `__init__` Mechanism

**Why This Matters**: View/MaterializedView inherit from Table, so they must follow Table's special initialization protocol.

##### Table's Singleton Pattern

SQLAlchemy's Table implements a **singleton pattern** - the same table name in the same MetaData returns the same instance:

```python
metadata = MetaData()
table1 = Table('users', metadata, Column('id', Integer))
table2 = Table('users', metadata)  # Returns table1, not a new instance
assert table1 is table2  # True
```

##### The Double-Initialization Problem

Python's object creation has two steps:

1. `__new__` creates the instance
2. `__init__` initializes the instance

**Problem**: Python automatically calls `__init__` after `__new__` returns, which would cause double initialization!

```python
# User calls
table = Table('users', metadata, Column('id', Integer))

# What happens:
1. Table.__new__() is called
   └─> Returns an instance (new or existing)
2. Python automatically calls table.__init__()  # ← Can't be prevented!
```

##### Table's Solution: The `_no_init` Guard

Table uses a clever `_no_init` parameter to control initialization:

```python
class Table:
    def __new__(cls, *args, **kw):
        return cls._new(*args, **kw)

    @classmethod
    def _new(cls, *args, **kw):
        key = _get_table_key(name, schema)

        if key in metadata.tables:
            # Table already exists
            table = metadata.tables[key]
            if extend_existing:
                table._init_existing(*args, **kw)  # Update via special method
            return table  # Python will call __init__, but _no_init=True will skip it
        else:
            # New table
            table = object.__new__(cls)
            metadata._add_table(name, schema, table)

            # Explicitly call __init__ with _no_init=False
            table.__init__(name, metadata, *args, _no_init=False, **kw)

            return table  # Python will call __init__ again, but _no_init=True will skip it

    def __init__(self, ..., _no_init: bool = True, **kw):
        if _no_init:
            return  # Skip initialization

        # Real initialization logic...
```

##### Call Flow Analysis

**Scenario 1: Creating New Table**

```plain text
table = Table('users', metadata, Column('id', Integer))
    ↓
Table.__new__()
    ↓
Table._new()
    ↓
table = object.__new__(Table)
metadata._add_table('users', None, table)
    ↓
table.__init__(..., _no_init=False)  # 1st call: Explicit, does initialization
    ↓
return table
    ↓
table.__init__(...)  # 2nd call: Python automatic, _no_init=True (default), skips
    ↓
Done
```

**Scenario 2: Getting Existing Table**

```plain text
table = Table('users', metadata)
    ↓
Table._new()
    ↓
table = metadata.tables['users']  # Already exists
return table  # No explicit __init__ call
    ↓
table.__init__(...)  # Python automatic call, _no_init=True (default), skips
    ↓
Done
```

**Scenario 3: Updating Existing Table (extend_existing=True)**

```plain text
table = Table('users', metadata, Column('name', Integer), extend_existing=True)
    ↓
Table._new()
    ↓
table = metadata.tables['users']  # Already exists
table._init_existing(Column('name', Integer), ...)  # Update via special method
return table
    ↓
table.__init__(...)  # Python automatic call, _no_init=True, skips
    ↓
Done
```

##### View/MaterializedView Implementation Requirements

**Key Requirements**:

1. **Must accept `_no_init` parameter** in `__init__` signature
2. **Must check `_no_init` at the start** of `__init__` and return early if True
3. **Must pass `_no_init=False`** when calling `super().__init__()`
4. **Must override `_init_existing`** to handle View-specific parameters (definition, columns)
5. **Must explicitly declare `keep_existing` and `extend_existing`** parameters for API clarity

**Correct Implementation Pattern**:

```python
class View(Table):
    def __init__(
        ...
        keep_existing: bool = False,      # Explicit declaration
        extend_existing: bool = False,    # Explicit declaration
        _no_init: bool = True,            # Required!
        **kwargs: Any,
    ) -> None:
        # 1. Check _no_init first (like Table does)
        if _no_init:
            return

        # 2. View-specific initialization
        if definition is None:
            raise ValueError("View definition is required")
        view_info = {TableObjectInfoKey.TABLE_KIND: TableKind.VIEW}
        view_info.update(self._process_definition(definition))
        kwargs.setdefault("info", {}).update(view_info)

        # 3. Call parent with _no_init=False (not _no_init=_no_init!)
        super().__init__(name, metadata, *all_columns,
                        schema=schema, comment=comment,
                        keep_existing=keep_existing,
                        extend_existing=extend_existing,
                        _no_init=False,  # Always False here!
                        **kwargs)

    def _init_existing(self, *args, **kwargs):
        """Handle extend_existing=True case"""
        # Extract View-specific parameters
        definition = kwargs.pop('definition', None)
        columns = kwargs.pop('columns', None)

        # Update definition
        if definition is not None:
            view_info = self._process_definition(definition)
            self.info.update(view_info)
        # ... other attributes

        # Call parent (Table will handle Column objects in args)
        super()._init_existing(*args, **kwargs)
```

**Common Mistakes to Avoid**:

❌ **Wrong**: Not checking `_no_init` at the start

```python
def __init__(self, ..., _no_init: bool = True, **kwargs):
    # Doing work before checking _no_init
    view_info = {...}  # This will run on every automatic call!

    if _no_init:
        return
```

❌ **Wrong**: Not declaring `keep_existing`/`extend_existing`

```python
def __init__(self, ..., **kwargs):  # They're hidden in kwargs
    # Users won't see these parameters in IDE autocomplete
    super().__init__(..., **kwargs)
```

✅ **Correct**: Follow the pattern above

##### Why `keep_existing` and `extend_existing` Must Be Explicit

**Problem**: These parameters control singleton behavior but were hidden in `**kwargs`.

**Solution**: Explicitly declare them in the signature.

**Benefits**:

- ✅ IDE autocomplete shows these parameters
- ✅ Type checkers can validate them
- ✅ Documentation is clearer
- ✅ API is consistent with Table

**Behavior**:

| Mode                                                         | Behavior                                                                                 | Use Case                                        |
| ------------------------------------------------------------ | ---------------------------------------------------------------------------------------- | ----------------------------------------------- |
| **Default** (`extend_existing=False`, `keep_existing=False`) | Return existing instance, don't update parameters. If Column args provided, raise error. | Normal usage: get reference to existing view    |
| **`keep_existing=True`**                                     | Return existing instance, ignore all new parameters (even Column args)                   | Optional view definition (create if not exists) |
| **`extend_existing=True`**                                   | Return existing instance, update parameters via `_init_existing`                         | Update existing view definition                 |

**Example**:

```python
# First creation
view1 = View('v1', metadata, definition='SELECT 1', comment='First')

# Default: returns same instance, doesn't update
view2 = View('v1', metadata, definition='SELECT 2', comment='Second')
assert view1 is view2
assert view2.definition == 'SELECT 1'  # Not updated
assert view2.comment == 'First'  # Not updated

# extend_existing: updates parameters
view3 = View('v1', metadata, definition='SELECT 3', comment='Third', extend_existing=True)
assert view1 is view3
assert view3.definition == 'SELECT 3'  # Updated!
assert view3.comment == 'Third'  # Updated!
```

##### Summary: Key Takeaways

1. **Table uses singleton pattern** - same name returns same instance
2. **`_no_init` parameter** controls whether initialization runs
3. **`__init__` is called twice** for new objects:
   - First: explicit call with `_no_init=False` (does initialization)
   - Second: Python automatic call with `_no_init=True` (skips)
4. **`__init__` is called once** for existing objects:
   - Only: Python automatic call with `_no_init=True` (skips)
5. **View/MaterializedView must**:
   - Check `_no_init` at start of `__init__`
   - Pass `_no_init=False` to `super().__init__()`
   - Override `_init_existing` for `extend_existing` support
   - Explicitly declare `keep_existing` and `extend_existing`
6. **`_init_existing`** is the proper way to update existing instances
7. **Code reuse**: Extract `_process_definition` to avoid duplication

#### Approach B: `View`/`MaterializedView` Inherits from `SchemaItem` ❌ **Not Adopted**

**Pros**:

- Lighter-weight, clearly distinguished from Table.
- Does not depend on Table's complex mechanisms.

**Cons**:

- Cannot leverage Table's infrastructure (columns, reflection, comparison, etc.).
- Requires significant custom logic for integration.

### 4.2 Reflection Layer (`reflection.py` and `dialect.py`)

#### Core Strategy

**Why must `_setup_parser` and `reflect_table` be overridden?**

1. **`_setup_parser`**: The core method of MySQL Dialect (with `@reflection.cache`), responsible for parsing Table attributes (using `SHOW CREATE TABLE` in MySQL) and returning `ReflectedState`.
2. **`reflect_table`**: SQLAlchemy's standard reflection entry point, `Table.autoload_with=engine` will eventually call it.

**Problem**: The standard process only knows how to reflect TABLEs, not how to obtain specific attributes of VIEWs/MVs (e.g., definition).

**Solution**:

1. **Override `_setup_parser`**: Return different `ReflectedState` subclasses based on the object type.
   - Query `table_kind` (only once, leveraging `@reflection.cache`).
   - Dispatch to `_setup_table_parser`, `_setup_view_parser`, `_setup_mv_parser`.
   - Return `ReflectedState`, `ReflectedViewState`, `ReflectedMVState` respectively.
2. **Override `reflect_table`**: Supplement VIEW/MV specific attributes.
   - Call the parent class to complete standard reflection (columns, constraints, etc.).
   - Get `table_kind` from the cached `ReflectedState`.
   - Set `table.info['table_kind']`.

**Key Design Points**:

1. `reflect_table()` is the core entry point and must be overridden to supplement VIEW/MV specific attributes.
2. Leverage existing caching mechanism:
   - `_setup_parser` has `@reflection.cache`.
   - Query the database only once; subsequent reads are from cache.
3. Does not affect the use of ordinary Tables.

#### Approach A: Override `_setup_parser` and `reflect_table` ⭐ **Adopted**

**Step 1: Define `ReflectedState` Subclasses**

```python
# engine/interfaces.py

@dataclass
class ReflectedState:
    """Reflected information for a Table (base class). Similar to MySQL's implementation."""
    table_name: Optional[str] = None
    columns: list[ReflectedColumn] = field(default_factory=list)
    table_options: dict[str, str] = field(default_factory=dict)
    keys: list = field(default_factory=list)
    fk_constraints: list = field(default_factory=list)

    @property
    def table_kind(self) -> str:
        return 'TABLE'

@dataclass
class ReflectedViewState(ReflectedState):
    """Reflected information for a View"""
    definition: str
    security: Optional[str] = None

    # Views have no primary keys, foreign keys, or indexes
    keys: list = field(default_factory=list, init=False)
    fk_constraints: list = field(default_factory=list, init=False)

    @property
    def table_kind(self) -> str:
        return 'VIEW'

@dataclass
class ReflectedMVState(ReflectedState):
    """Reflected information for a Materialized View"""
    definition: str = ''
    partition_info: Optional[ReflectedPartitionInfo] = None
    distribution_info: Optional[ReflectedDistributionInfo] = None
    refresh_info: Optional[str] = None

    # MVs have no primary keys or foreign keys
    keys: list = field(default_factory=list, init=False)
    fk_constraints: list = field(default_factory=list, init=False)

    @property
    def table_kind(self) -> str:
        return 'MATERIALIZED_VIEW'
```

**Step 2: Override `_setup_parser`**

```python
# dialect.py

class StarRocksDialect(MySQLDialect_pymysql):

    @reflection.cache  # Keep caching
    def _setup_parser(
        self, connection, table_name, schema=None, info_cache=None
    ) -> ReflectedState:
        """
        Override to return different ReflectedState subclasses based on object type.

        Key: Query table_kind only once here, leveraging @reflection.cache.
        """
        # 1. Query object type (only once)
        table_kind = self._get_table_kind_from_db(connection, table_name, schema)

        # 2. Dispatch based on type
        if table_kind == 'VIEW':
            return self._setup_view_parser(connection, table_name, schema, info_cache)
        elif table_kind == 'MATERIALIZED_VIEW':
            return self._setup_mv_parser(connection, table_name, schema, info_cache)
        else:
            return self._setup_table_parser(connection, table_name, schema, info_cache)

    def _setup_table_parser(self, connection, table_name, schema, info_cache):
        """Parse Table (call parent class)"""
        # Query basic TABLE information, return ReflectedState

    def _setup_view_parser(self, connection, table_name, schema, info_cache):
        """Parse View (get from information_schema.views)"""
        # Query VIEW information, return ReflectedViewState
        # ...

    def _setup_mv_parser(self, connection, table_name, schema, info_cache):
        """Parse MV (get from information_schema.materialized_views)"""
        # Query MV information, return ReflectedMVState
        # ...

    def _get_table_kind_from_db(self, connection, table_name, schema) -> str:
        """
        Query object type from the database (without cache).
        Only called once in _setup_parser.

        StarRocks: VIEW and MV both show as 'VIEW' in information_schema.tables.
        A secondary check against information_schema.materialized_views is needed to distinguish.
        """
        # 1. Query information_schema.tables
        table_rows = self._read_from_information_schema(
            connection, "tables", table_schema=schema, table_name=table_name
        )

        table_type = table_rows[0].TABLE_TYPE

        # 2. BASE TABLE → "TABLE"
        if table_type == 'BASE TABLE':
            return "TABLE"

        # 3. VIEW → Further Distinguish
        if table_type == 'VIEW':
            mv_rows = self._read_from_information_schema(
                connection, "materialized_views",
                table_schema=schema, table_name=table_name
            )
            return "MATERIALIZED_VIEW" if mv_rows else "VIEW"
```

**Step 3: Provide `get_table_kind` Method**

```python
# dialect.py

class StarRocksDialect(MySQLDialect_pymysql):

    @reflection.cache
    def get_table_kind(self, connection, table_name, schema=None) -> str:
        """
        Get the object's table_kind (from cache).

        Reuse _setup_parser's cache via _parsed_state_or_create.
        """
        parsed_state = self._parsed_state_or_create(connection, table_name, schema)
        return parsed_state.table_kind  # Use @property
```

**Step 4: Override `reflect_table`**

```python
# reflection.py

class StarRocksInspector(Inspector):
    def reflect_table(self, table, table_name, schema=None, **kwargs):
        """
        Override to set VIEW/MV specific attributes.

        Key roles:
        1. Call parent class to complete standard reflection (columns, constraints, etc.)
        2. Get table_kind from cache
        3. Set table.info['table_kind']
        4. Set VIEW/MV specific attributes
        """
        # 1. Call parent class (will call get_pk_constraints, etc., which will trigger _setup_parser)
        super().reflect_table(table, table_name, schema, **kwargs)

        # 2. Get table_kind and parsed_state (from cache)
        parsed_state = self.dialect._parsed_state_or_create(self.bind, table_name, schema)
        table_kind = parsed_state.table_kind

        # 3. Set info['table_kind']
        table.info['table_kind'] = table_kind

        # 4. Set specific attributes based on type
        if table_kind == 'VIEW':
            self._reflect_view_attributes(table, parsed_state)
        elif table_kind == 'MATERIALIZED_VIEW':
            self._reflect_mv_attributes(table, parsed_state)

    def _reflect_view_attributes(self, table, view_state):
        """Set View specific attributes from ReflectedViewState"""
        table.info['definition'] = view_state.definition
        if view_state.security:
            table.dialect_options.setdefault('starrocks', {})['security'] = view_state.security

    def _reflect_mv_attributes(self, table, mv_state: ReflectedMVState):
        """Set MV specific attributes from ReflectedMVState"""
        # Set definition
        table.info[DEFINITION] = mv_state.definition

        # Set dialect-specific MV attributes explicitly
        dialect_opts = table.dialect_options.setdefault(DialectName, {})
        if mv_state.partition_info:
            dialect_opts[PARTITION_BY] = str(mv_state.partition_info)
        if mv_state.distribution_info:
            dialect_opts[DISTRIBUTED_BY] = str(mv_state.distribution_info)
        if mv_state.refresh_info:
            dialect_opts[REFRESH] = str(mv_state.refresh_info)
        if mv_state.order_by:
            dialect_opts[ORDER_BY] = mv_state.order_by
        if mv_state.properties:
            dialect_opts[PROPERTIES] = mv_state.properties
```

**Pros**:

- **Optimal Performance**: Only query the database once (in `_setup_parser`), and then read from cache.
- **OOP-Based**: Differences are handled naturally through inheritance and polymorphism (e.g., `ReflectedViewState.keys` is always empty).
- **Good Extensibility**: Adding new object types only requires adding new State subclasses.
- **Perfectly Compatible with SQLAlchemy's `Table.autoload_with=engine` Mechanism**:

**Cons**:

- Requires understanding MySQL Dialect's `_setup_parser` Mechanism.

**Why This Design**:

1. **Leverage Existing Mechanisms**:

   - All `get_*` methods in MySQL call `_parsed_state_or_create()`.
   - `_parsed_state_or_create()` internally calls `_setup_parser()` (with `@reflection.cache`).
   - We override `_setup_parser`, returning different State subclasses based on type.

2. **No Need to Override `get_*` Methods**:
   - `ReflectedViewState.keys = []` (empty list)
   - The parent class's `get_pk_constraints()` will iterate through `keys`, naturally returning empty.
   - Similarly: `get_foreign_keys()`, `get_indexes()` do not need to be modified.

#### Approach B: Query Type in Each `get_*` Method ❌ **Not Adopted**

**Idea**: Override methods like `get_pk_constraints()`, query `table_kind` internally, and return the corresponding result.

**Cons**:

- Code redundancy: requires overriding multiple `get_*` methods
- Not elegant: does not leverage OOP polymorphism
- Same performance: still requires one DB query + caching

#### Approach C: Independent reflect_view and reflect_materialized_view ❌ **Not Adopted**

**Cons**:

- **Cannot be Integrated with `Table.autoload_with=engine`** (Fatal Flaw)
- Users need to call different methods based on type.

#### Special Handling: SECURITY Reflection

**Problem**: StarRocks' `information_schema.views.security_type` is always empty (or `NONE` only).

**Solution**: Parse `SECURITY` clause from `SHOW CREATE VIEW` output using regex pattern `_VIEW_SECURITY_PATTERN`.

#### How to Distinguish VIEW and MV

In StarRocks' `information_schema.tables`, both VIEW and MV have `'VIEW'` for `table_kind`. They need to be distinguished by `information_schema.materialized_views`:

```python
def _get_table_kind_from_db(self, connection, table_name, schema) -> str:
    """
    Query object type from the database (without cache).
    Only called once in _setup_parser.
    """
    # 1. Query information_schema.tables
    table_rows = self._read_from_information_schema(
        connection, "tables", table_schema=schema, table_name=table_name
    )

    table_type = table_rows[0].TABLE_TYPE

    # 2. BASE TABLE → "TABLE"
    if table_type == 'BASE TABLE':
        return "TABLE"

    # 3. VIEW → Further Distinguish
    if table_type == 'VIEW':
        mv_rows = self._read_from_information_schema(
            connection, "materialized_views",
            table_schema=schema, table_name=table_name
        )
        return "MATERIALIZED_VIEW" if mv_rows else "VIEW"
```

**Query Flowchart**:

```plain text
information_schema.tables (TABLE_TYPE)
    ├─ 'BASE TABLE' → Return "TABLE"
    └─ 'VIEW' → Further Query
        └─ information_schema.materialized_views
            ├─ Exists → Return "MATERIALIZED_VIEW"
            └─ Does Not Exist → Return "VIEW"
```

### 4.3 Compiler Layer (`dialect.py` - DDLCompiler)

#### Approach A: Extend `visit_create_table` + Reuse Independent Visit Methods ⭐ **Adopted**

```python
class StarRocksDDLCompiler(MySQLDDLCompiler):

    def visit_create_table(self, create: CreateTable, **kw: Any) -> str:
        """Check table_kind, dispatch to corresponding method"""
        table: Table = create.element
        table_kind: str = table.info.get('table_kind', 'TABLE')

        if table_kind == 'VIEW':
            return self._compile_create_view_from_table(table, create, **kw)
        elif table_kind == 'MATERIALIZED_VIEW':
            return self._compile_create_mv_from_table(table, create, **kw)
        else:
            # Original TABLE logic
            return self._compile_create_table_original(table, create, **kw)

    def _compile_create_view_from_table(
        self,
        table: Table,
        create: CreateTable,
        **kw: Any
    ) -> str:
        """Compile CREATE VIEW from Table object"""
        definition: str = table.info['definition']
        security: Optional[str] = table.dialect_options.get('starrocks', {}).get('security')
        # Construct SQL...
        sql = f"CREATE VIEW {self.preparer.format_table(table)}\n"
        # ... Omit detailed implementation
        return sql

    def visit_create_view(self, create: CreateTable, **kw: Any) -> str:
        """
        Handle CreateView DDL element.
        Directly call _compile_create_view_from_table, as View inherits from Table.
        """
        return self._compile_create_view_from_table(create.element, create, **kw)

    def visit_create_materialized_view(self, create: CreateTable, **kw: Any) -> str:
        """Handle CreateMaterializedView DDL element"""
        return self._compile_create_mv_from_table(create.element, create, **kw)
```

**Pros**:

- Code reuse, avoiding duplication.
- Support both creation methods.
- Logical concentration, easy to maintain.

**Cons**:

- Requires understanding DDL elements and Table object relationships.

#### Approach B: Maintain Independent Visit Methods, Do Not Extend `visit_create_table` ❌ **Not Adopted**

**Pros**:

- Independent code, no interference.

**Cons**:

- Code duplication.
- `Table(info={'table_kind': 'VIEW'})` cannot work.

### 4.4 Autogenerate/Compare Layer (`compare.py`)

#### Approach A: Maintain Separation, Share Comparison Logic ⭐ **Adopted**

**Reason**:

1. Alembic's `_autogen_for_tables` only calls `get_table_names()`.
2. Different default values and comparison rules for TABLE/VIEW/MV.
3. Need separate entry points to trigger their respective comparisons.

**Critical**: Use `include_object` callback to filter View/MV from Alembic's built-in `_autogen_for_tables`.

#### Key Implementation Details

**1. Use View/MaterializedView Objects for Reflection**

When reflecting views/MVs in comparison functions, create View/MaterializedView objects (not plain Table objects):

- Provides semantic clarity and type consistency
- Example: `t = View(vname, removal_metadata, definition='', schema=s)`

**2. Column Extraction Function**

`extract_view_columns()` is a module-level function (not View member method):

- Works with both View objects and Table objects representing views
- Reflected views are Table objects, so module-level function is more flexible
- Used by Alembic operations to serialize view columns for migration scripts

**3. Warning Strategy**

Use `warnings.warn(UserWarning)` instead of `logger.warning` for unsupported operations:

- More visible to users (appears in console by default)
- Can be controlled by Python's warning filters
- Applies to: comment changes, security changes, column-only changes, and all MV changes

**Pre-defined Function**: StarRocks dialect provides `starrocks.alembic.include_object_for_view_mv` for convenience.

```python
# compare.py
def include_object_for_view_mv(object, name, type_, reflected, compare_to):
    """
    Filter objects for autogenerate - exclude View/MV from table comparisons.

    View/MV are handled by their own comparators (_autogen_for_views/mvs).
    """
    if type_ == "table" and compare_to is not None:
        table_kind = compare_to.info.get('table_kind', 'TABLE')
        if table_kind in ('VIEW', 'MATERIALIZED_VIEW'):
            return False  # Let _autogen_for_views/mvs handle them
    return True

# Export in __init__.py for easy access
from .compare import include_object_for_view_mv

# User configuration in env.py
from starrocks.alembic import include_object_for_view_mv

context.configure(
    # ...
    include_object=include_object_for_view_mv,
)

@comparators_dispatch_for_starrocks("schema")
def _autogen_for_views(autogen_context, upgrade_ops, schemas):
    """Specifically handle views"""
    for schema in schemas:
        # Database views
        conn_views = {name: inspector.reflect_table(name, schema)
                     for name in inspector.get_view_names(schema)}

        # Metadata views
        meta_views = {t.name: t for t in metadata.tables.values()
                     if t.info.get('table_kind') == 'VIEW'}

        _compare_objects(conn_views, meta_views, 'VIEW', ...)

@comparators_dispatch_for_starrocks("schema")
def _autogen_for_mvs(autogen_context, upgrade_ops, schemas):
    """Specifically handle MVs"""
    for schema in schemas:
        conn_mvs = {name: inspector.reflect_table(name, schema)
                   for name in inspector.get_materialized_view_names(schema)}
        meta_mvs = {t.name: t for t in metadata.tables.values()
                   if t.info.get('table_kind') == 'MATERIALIZED_VIEW'}

        _compare_objects(conn_mvs, meta_mvs, 'MATERIALIZED_VIEW', ...)

def _compare_objects(conn_objs, meta_objs, object_kind, ...):
    """Uniform comparison logic"""
    # Compare definition, comment, dialect_options, etc.
    # Use different default values based on object_kind
```

**Pros**:

- Clear entry points.
- Can set different default values and comparison rules for different object types.
- Share core comparison logic.

**Cons**:

- Need to maintain three separate entry points.

#### Approach B: Unify to Table Comparator ❌ **Not Adopted**

**Pros**:

- Only one entry point.

**Cons**:

- VIEW/MV will not be automatically detected.
- Difficult to handle default value differences for different object types.

### 4.5 Operations Layer (`ops.py`)

#### Approach A: Independent Ops + from_table() Method ⭐ **Adopted**

Take the view for example. MV is similar.

```python
@Operations.register_operation("create_view")
class CreateViewOp(ops.MigrateOperation):
    def __init__(
        self,
        view_name: Optional[str] = None,
        definition: Optional[str] = None,
        table: Optional[Table] = None,
        **kwargs: Any
    ) -> None:
        """
        Support two ways:
        1. CreateViewOp(view_name, definition, ...)
        2. CreateViewOp(table=view_table)
        """
        if table is not None:
            self.view_name: str = table.name
            self.definition: Optional[str] = table.info.get('definition')
            self.schema: Optional[str] = table.schema
            # ... Extract other attributes from table
        else:
            self.view_name = view_name
            self.definition = definition

    @classmethod
    def from_table(cls, table: Table) -> "CreateViewOp":
        """Create Op from Table object"""
        return cls(table=table)
```

**Pros**:

- Clear API.
- Support two construction methods.
- No need to modify `CreateTableOp`.

**Cons**:

- Need to maintain multiple Op classes.

#### Key Implementation Details

**1. AlterViewOp Column Support**

`AlterViewOp` includes `columns` and `existing_columns` parameters:

- Columns can only change together with definition in StarRocks
- Needed for rendering complete view information and supporting reverse operations
- Future compatibility if StarRocks adds column-only ALTER support

**3. Metadata Handling in Operations**

- Use temporary `MetaData()` (via `op.to_view()`) instead of using `operations.get_context().opts['target_metadata']`
- Note: Operations create Views in new MetaData, avoiding singleton/re-initialization issues

#### Approach B: Extend `CreateTableOp` ❌ **Not Adopted**

**Cons**:

- `CreateTableOp` semantics are unclear.
- Does not align with Alembic's design.

### 4.6 Render Layer (`render.py`)

Maintain existing logic, render as dedicated operations:

- `CreateViewOp` → `op.create_view(...)`
- `CreateMaterializedViewOp` → `op.create_materialized_view(...)`

Both ways users write work:

```python
# Recommended way
op.create_view('my_view', 'SELECT ...', starrocks_security='DEFINER')

# Also works
op.create_table('my_view',
                info={'table_kind': 'VIEW', 'definition': 'SELECT ...'},
                starrocks_security='DEFINER')
```

## 5. Detailed Design Points

### 5.1 Schema Layer Key Points

**Design Points**:

1. **Parameter Passing**: All `starrocks_*` parameters are passed to `Table.__init__` via `kwargs`.
2. **Info Setting**: Set `info` before calling `super().__init__`.
3. **@property Access**: Provide convenient properties to get values from Table.
4. **Column Extraction**: `extract_view_columns()` is a module-level function that works with both View objects and Table objects representing views. This design choice accommodates reflected views which are Table objects.

**Example**:

```python
# User creation
my_view = View('v1', 'SELECT * FROM t1',
               metadata=metadata,
               comment='My view',
               starrocks_security='DEFINER')

# Equivalent to
my_view = Table('v1', metadata,
                info={'table_kind': 'VIEW', 'definition': 'SELECT * FROM t1'},
                comment='My view',
                starrocks_security='DEFINER')
```

### 5.2 Reflection Layer Key Points

**Design Points**:

1. **`_setup_parser` is Core**:

   - With `@reflection.cache`, ensures performance.
   - Returns different `ReflectedState` subclasses based on `table_kind`.
   - Queries the database only once.

2. **Polymorphism of `ReflectedState` Subclasses**:

   - `ReflectedViewState.keys = []` (empty list)
   - The parent class's `get_pk_constraints()` iterates through `keys`, naturally returning empty.
   - No need to override `get_*` methods.

3. **Actual Call Flow**:

   ```python
   Table.autoload_with(engine)
       ↓
   Inspector.reflect_table('my_view')
       ↓
       super().reflect_table()  # Parent method
           ↓
           get_pk_constraints('my_view')
               ↓
               _parsed_state_or_create('my_view')
                   ↓
                   _setup_parser('my_view')  # ✅ Query DB once, return ReflectedViewState
                       ├─ _get_table_kind_from_db() → "VIEW"
                       └─ _setup_view_parser() → ReflectedViewState(keys=[])
               ↓
           get_foreign_keys('my_view')
               ↓
              _parsed_state_or_create('my_view')
                   ↓
                   _setup_parser('my_view')  # ✅ Return from cache
               ↓
           # ... Other get_* methods similarly
           ↓
       # reflect_table continues:
           ↓
       get_table_kind('my_view')  # Get from cached state
           ↓
        Set table.info['table_kind']
   ```

4. **Column Information**: Column details (including comments) are handled by SQLAlchemy's standard reflection via `get_columns()`, which returns `ReflectedColumn` dictionaries from `view_state.columns`.

5. **SECURITY Reflection**: Since `information_schema.views.security_type` is empty in StarRocks, security mode is extracted from `SHOW CREATE VIEW` output using regex pattern in `_setup_view_parser()`.

### 5.3 Compiler Layer Key Points

**Design Points**:

1. **`visit_create_table` Extension**: Check `table_kind`, dispatch to corresponding method.
2. **Code Reuse**: `visit_create_view` directly calls `_compile_create_view_from_table`.
3. **Attribute Extraction**: Extract from `table.info` and `table.dialect_options`.
4. **Refresh Parsing**: Parse `starrocks_refresh` into `REFRESH IMMEDIATE ASYNC` etc.
5. **Error Handling**: Throw `CompileError` if required attributes are missing.

**Example**:

```python
def _compile_create_view_from_table(self, table, create, **kw):
    definition = table.info['definition']
    opts = table.dialect_options.get('starrocks', {})

    sql = f"CREATE VIEW {table.name}\n"
    if table.comment:
        sql += f"COMMENT {quote(table.comment)}\n"
    if security := opts.get('security'):
        sql += f"SECURITY {security}\n"
    sql += f"AS\n{definition}"
    return sql
```

**Additional Compiler Notes**:

- Use case-insensitive extraction for dialect options to avoid casing issues:
  - Prefer a helper like `extract_dialect_options_as_case_insensitive(table)` instead of reading `table.dialect_options` directly.
- Reuse `post_create_table(...)` to append common clauses (COMMENT, PARTITION BY, DISTRIBUTED BY, ORDER BY, PROPERTIES, REFRESH for MV) for consistent SQL composition.

### 5.4 Compare Layer Key Points

**Design Points**:

1. **Separate Entry Points**: TABLE/VIEW/MV maintain independent `autogen_for_*` entry points.
2. **Uniform Object Type**: All are Table objects, but `table_kind` is different.
3. **MV ALTER Limitations**: Based on [ALTER MATERIALIZED VIEW docs](https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW/):
   - ✅ **Mutable**: `refresh`, `properties` (can use ALTER)
   - ❌ **Immutable**: `definition`, `partition_by`, `distributed_by`, `order_by` (cannot ALTER, log warning)
4. **Critical Filter via `include_object`**: Must exclude View/MV from Alembic's `_autogen_for_tables`.
   - **Problem**: `sorted_tables` includes all Table objects (View/MV inherit from Table).
   - **Solution**: Provide pre-defined `include_object_for_view_mv` and `combine_include_object`.
   - **Why**: `@comparators.dispatch_for` is append-only, can't override internal comparator.
   - **Implementation**: Define in `compare.py`, document in README.
5. **User Custom Filters**: Must respect user's `include_object` in our comparators.
   - **Problem**: `autogen_for_view/mv` needs to honor user's custom filter logic.
   - **Solution**: Call `autogen_context.run_object_filters()` for each View/MV.
   - **Why**: Alembic's internal comparators call this, ours must too.
6. **Shared Comparison Logic**: Extract common functions for comparison.
7. **Default Value Handling**: Use different default values for different object types.
8. **Attribute Normalization**: Normalize attributes before comparison (e.g., SQL formatting).
9. **Reflection Object Type**: When reflecting views/MVs for comparison, create View/MaterializedView objects (not plain Table objects) for semantic clarity and type consistency.
10. **Warning Strategy**: Use `warnings.warn(UserWarning)` for unsupported ALTER operations (more visible than `logger.warning`).

**Key Logic**:

```python
# Pre-defined filter functions (in compare.py)
def include_object_for_view_mv(object, name, type_, reflected, compare_to):
    """Filter View/MV from table comparisons."""
    if type_ == "table" and compare_to is not None:
        table_kind = compare_to.info.get('table_kind', 'TABLE')
        if table_kind in ('VIEW', 'MATERIALIZED_VIEW'):
            return False
    return True

def combine_include_object(user_filter=None):
    """Combine user's filter with View/MV filter."""
    def combined(object, name, type_, reflected, compare_to):
        if not include_object_for_view_mv(object, name, type_, reflected, compare_to):
            return False
        if user_filter is not None:
            return user_filter(object, name, type_, reflected, compare_to)
        return True
    return combined

# Custom comparators must call run_object_filters
@comparators.dispatch_for("schema")
def _autogen_for_views(autogen_context, upgrade_ops, schemas):
    for schema_name in schemas:
        # Get Views from metadata
        for view in _get_views_from_metadata(autogen_context, schema_name):
            # ✅ Must call run_object_filters to respect user's filter
            if not autogen_context.run_object_filters(
                None, view.name, "view", False, view
            ):
                continue  # User chose to skip this view

            # Continue comparison logic...

# Usage in env.py - Simple case
from starrocks.alembic import include_object_for_view_mv
context.configure(include_object=include_object_for_view_mv)

# Usage in env.py - With custom filter
from starrocks.alembic import combine_include_object

def my_filter(object, name, type_, reflected, compare_to):
    if name.startswith('temp_'):
        return False
    return True

context.configure(include_object=combine_include_object(my_filter))
```

#### MV ALTER Capabilities

Based on [StarRocks ALTER MATERIALIZED VIEW documentation](https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW/), MV supports limited ALTER operations:

**✅ Supported ALTER Operations** (from schema comparison perspective):

1. **`refresh`** - Modify refresh strategy

   - Syntax: `ALTER MATERIALIZED VIEW mv_name REFRESH {ASYNC|MANUAL} ...`
   - Can be detected in schema comparison and generate `AlterMaterializedViewOp`

2. **`properties`** - Modify properties
   - Syntax: `ALTER MATERIALIZED VIEW mv_name SET ("key" = "value", ...)`
   - Can be detected in schema comparison and generate `AlterMaterializedViewOp`

**❌ Not Supported (Changes Generate Warning Only)**:

The following attributes cannot be altered via `ALTER MATERIALIZED VIEW`:

1. **`definition`** - Query definition (requires DROP + CREATE)
2. **`partition_by`** - Partition strategy (requires DROP + CREATE)
3. **`distributed_by`** - Distribution strategy (requires DROP + CREATE)
4. **`order_by`** - Order by clause (requires DROP + CREATE)
5. **`comment`** - Comment (not mentioned in ALTER MV docs)
6. **`columns`** - Column definitions (not mentioned in ALTER MV docs)

**Comparison Strategy**:

```python
def _compare_mv(autogen_context, upgrade_ops, schema, mv_name, conn_mv, metadata_mv):
    """
    Compare MV and generate operations.

    Strategy:
        - Only refresh and properties changes → Generate AlterMaterializedViewOp
        - Other attribute changes → Generate warning only (no operations)
    """

    # Check immutable attributes (cannot ALTER)
    if _compare_mv_definition(conn_mv, metadata_mv):
        warnings.warn(
            f"StarRocks does not support altering MV definition; "
            f"definition changes detected for {mv_name} will be ignored.",
            UserWarning
        )

    ...

    # Check mutable attributes (can ALTER)
    alter_op = None

    # Refresh (can ALTER)
    refresh_changed, conn_refresh, meta_refresh = _compare_mv_refresh_with_reverse(conn_mv, metadata_mv)
    if refresh_changed:
        alter_op = alter_op or AlterMaterializedViewOp(mv_name, schema)
        alter_op.refresh = meta_refresh
        alter_op.existing_refresh = conn_refresh

    # Properties (can ALTER)
    ...

    # Generate operation if needed
    if alter_op is not None:
        if not autogen_context.run_object_filters(
            mv_name, "materialized_view", False, alter_op
        ):
            return
        upgrade_ops.ops.append(alter_op)
```

**AlterMaterializedViewOp Design**:

```python
@Operations.register_operation("alter_materialized_view")
class AlterMaterializedViewOp(ops.MigrateOperation):
    """
    Alter a materialized view (mutable attributes only).

    Only supports: refresh, properties.
    Does not support: definition, partition_by, distributed_by, order_by, comment, columns.
    """

    def __init__(
        self,
        view_name: str,
        schema: Optional[str] = None,
        # Mutable attributes only
        refresh: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        # Reverse values for downgrade
        existing_refresh: Optional[str] = None,
        existing_properties: Optional[Dict[str, str]] = None,
    ):
        self.view_name = view_name
        self.schema = schema
        self.refresh = refresh
        self.properties = properties
        self.existing_refresh = existing_refresh
        self.existing_properties = existing_properties

    def reverse(self) -> "AlterMaterializedViewOp":
        return AlterMaterializedViewOp(
            self.view_name,
            self.schema,
            refresh=self.existing_refresh,
            properties=self.existing_properties,
            existing_refresh=self.refresh,
            existing_properties=self.properties,
        )
```

> **Note on Limitations**: StarRocks' `ALTER MATERIALIZED VIEW` statement has several limitations. The current integration only supports altering the `REFRESH` clause and `PROPERTIES`. Renaming the materialized view, changing its definition (`AS SELECT ...`), or modifying `PARTITION BY`, `DISTRIBUTED BY`, and `ORDER BY` clauses are not supported. For such changes, you must manually write a `DROP` and `CREATE` operation in your migration script.

### 5.5 Metadata Management Key Points

**Design Points**:

1. **Uniform Interface**: Use `metadata.tables` uniformly.
2. **Filter Logic**: Objects can be filtered by `table_kind`.

## 6. Implementation Roadmap

### Phase 1: Core Architecture (Core)

1. ✅ Add `ObjectKind` constants to `params.py`.
2. Modify `View`/`MaterializedView` to inherit from `Table`.
   - Simplify `__init__`, leverage Table's parameter handling.
   - Merge refresh parameters.
   - Add `@property` for convenient access.
3. Modify `ReflectedMVOptions`, merge refresh field.
4. Add unit tests.

### Phase 2: Compiler Support

1. Extend `visit_create_table`.
2. Implement `_compile_create_view_from_table`.
3. Implement `_compile_create_mv_from_table`.
4. Modify `visit_create_view` to reuse logic.
5. Test: Create View/MV using Table.

### Phase 3: Unified Reflection

1. Override `reflect_table` (Critical).
2. Implement `_reflect_view_attributes`.
3. Implement `_reflect_mv_attributes`.
4. Modify `get_pk_constraints` etc. methods (get kind from kw).
5. Test: autoload and reflection.

### Phase 4: Compare Refactoring

1. **Implement filter functions in `compare.py`**:
   - `include_object_for_view_mv`: Filter View/MV from table comparisons
   - `combine_include_object`: Helper to combine with user's custom filter
2. **Update README.md** to guide users to configure it in `env.py`.
3. Modify `_autogen_for_views` to use Table objects:
   - **Critical**: Call `autogen_context.run_object_filters()` for each View
4. Modify `_autogen_for_mvs` to use Table objects:
   - **Critical**: Call `autogen_context.run_object_filters()` for each MV
5. Implement `_compare_objects` unified logic.
6. Test: autogenerate detects changes and respects user's custom filters.

### Phase 5: Operations Enhancement

1. Modify `CreateViewOp` to support `table` parameter.
2. Add `from_table()` method.
3. Modify `CreateMaterializedViewOp`.
4. Test: Create Op from Table.

### Phase 6: Complete and Documentation

1. End-to-end testing.
2. Performance testing.
3. User documentation.
4. API documentation.

## 7. Key Considerations

### 7.1 refresh Parameter Handling

**User Input**:

```python
# Do not subdivide moment and type in refresh, pass it as a single string
MaterializedView('mv1', 'SELECT ...',
                starrocks_refresh='IMMEDIATE ASYNC')
```

**Internal Storage**: Unified as `dialect_options['starrocks']['refresh'] = 'IMMEDIATE ASYNC'`.

**Compile-time Parsing**: `'IMMEDIATE ASYNC'` → `REFRESH IMMEDIATE ASYNC`.

### 7.2 Columns Support

**Current**: TODO - Not Implemented

- View/MV columns are determined by the SELECT statement.
- Can be obtained via reflection (TODO).

**Future**: Consider supporting explicit column definitions (for type hints).

### 7.3 Default Value Handling

- `table_kind` in `info` defaults to `'TABLE'`.
- `table_kind` is automatically set during reflection.
- Pay attention to default value differences for different object types during comparison.

### 7.4 Performance Considerations

- `_setup_parser()` uses `@reflection.cache` to cache results.
- The same `table_name` is queried only once during a single reflection.
- `get_table_kind()` reuses the cache via `_parsed_state_or_create()`.

### 7.5 Error Handling

- Missing `definition` → `CompileError` during compilation.
- Incorrect `table_kind` → Raise ValueError exception.
- Reflection failure → Return `None` or raise an exception.

## 8. Example Code

### 8.1 User-Defined View/MV

**Method 1: String Definition** (for hand-written SQL, reflection, migrations):

```python
from sqlalchemy import MetaData
from starrocks import View, MaterializedView

metadata = MetaData()

# View
my_view = View(
    'user_view',
    'SELECT id, name FROM users WHERE active = 1',
    metadata=metadata,
    schema='test_sqla',
    comment='Active users',
    starrocks_security='DEFINER'
)

# MV
my_mv = MaterializedView(
    'user_stats_mv',
    'SELECT date, COUNT(*) FROM logs GROUP BY date',
    metadata=metadata,
    schema='test_sqla',
    comment='Daily stats',
    starrocks_partition_by='RANGE(date)',
    starrocks_distributed_by='HASH(date) BUCKETS 8',
    starrocks_refresh='IMMEDIATE ASYNC'
)
```

**Method 2: Selectable Object** (for complex queries, type safety):

```python
metadata = MetaData()
users = Table('users', metadata,
    Column('id', INTEGER, primary_key=True),
    Column('name', STRING(50)),
    Column('active', BOOLEAN)
)

# View with simple query
selectable_stmt = select(users.c.id, users.c.name).where(users.c.active == True)
my_view = View('user_view', selectable_stmt, metadata, comment='Active users')

# View with complex query (joins, aggregations)
orders = Table('orders', metadata,
               Column('user_id', INTEGER),
               Column('total', INTEGER))
selectable_stmt = (
    select(users.c.id, users.c.name, func.sum(orders.c.total).label('total'))
        .select_from(users.join(orders, users.c.id == orders.c.user_id))
        .group_by(users.c.id, users.c.name)
)
my_mv = MaterializedView(
    'user_stats_mv',
    selectable_stmt,
    metadata,
    starrocks_partition_by='RANGE(date)',
    starrocks_refresh='IMMEDIATE ASYNC'
)
```

**Table Method**:

```python
from sqlalchemy import Table, MetaData

metadata = MetaData()

my_view = Table(
    'user_view',
    metadata,
    schema='test_sqla',
    info={
        'object_kind': 'VIEW',
        'definition': 'SELECT id, name FROM users'
    },
    comment='Active users',
    starrocks_security='DEFINER'
)
```

**ORM Method**:

```python
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from starrocks import View

Base = declarative_base()

# Method 1: Directly define View table
class UserView(Base):
    __tablename__ = 'user_view'
    __table_args__ = {
        'info': {
            'table_kind': 'VIEW',
            'definition': 'SELECT id, name FROM users WHERE active = 1'
        },
        'starrocks_security': 'DEFINER'
    }

    # Define columns (optional, for type hints)
    id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    name: Mapped[str] = mapped_column(STRING(50))

# Method 2: Use __table__ directly to specify
class UserStatsView(Base):
    __table__ = View(
        'user_stats_view',
        'SELECT COUNT(*) as cnt FROM users',
        Base.metadata,
        comment='User statistics',
        starrocks_security='DEFINER'
    )
```

### 8.2 Reflection

```python
from sqlalchemy import create_engine, MetaData, Table

engine = create_engine('starrocks://...')
metadata = MetaData()

# Autoload - Automatically recognize object type
my_table = Table('my_view', metadata, autoload_with=engine, schema='test_sqla')
print(my_table.info['table_kind'])  # 'VIEW'
print(my_table.info['definition'])    # 'SELECT ...'
print(my_table.dialect_options['starrocks']['security'])  # 'DEFINER'
```

### 8.3 Autogenerate

**Migration generated after detecting changes**:

```python
def upgrade():
    op.create_view(
        'user_view',
        'SELECT id, name FROM users WHERE active = 1',
        schema='test_sqla',
        comment='Active users',
        starrocks_security='DEFINER'
    )

    op.create_materialized_view(
        'user_stats_mv',
        'SELECT date, COUNT(*) FROM logs GROUP BY date',
        schema='test_sqla',
        comment='Daily stats',
        starrocks_partition_by='RANGE(date)',
        starrocks_distributed_by='HASH(date) BUCKETS 8',
        starrocks_refresh='IMMEDIATE ASYNC'
    )
```

## 9. Summary

This design achieves:

1. **Unified Representation**: All objects are Tables, distinguished by `info["table_kind"]`.
2. **Concise Implementation**: Fully leverages Table's parameter handling mechanism.
3. **Flexible Interface**: Supports multiple definition methods (convenience class, Table, ORM).
4. **Clear Layering**: info (general) / comment (standard) / dialect_options (dialect-specific).
5. **Core Overrides**: `reflect_table` is key, supports autoload.
6. **Separated Comparison**: TABLE/VIEW/MV independent entry points, shared core logic.

This architecture maintains user interface friendliness while fully utilizing SQLAlchemy's infrastructure, providing a solid foundation for future extensions.
