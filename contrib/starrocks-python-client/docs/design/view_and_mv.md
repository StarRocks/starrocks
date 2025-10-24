# View and MaterializedView Architecture Design

## 1. Design Goals

To unify the representation of View and MaterializedView using the `Table` class, distinguishing them by `info["table_type"]` as TABLE/VIEW/MATERIALIZED_VIEW.

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
   - **General Attributes** → `table.info`: `table_type`, `definition`
   - **Standard Attributes** → `Table` standard attributes: `comment` (via `table.comment`)
   - **Dialect-Specific Attributes** → `table.dialect_options['starrocks']`: `partition_by`, `distributed_by`, `order_by`, `refresh`, `properties`, view `security` passed via `starrocks_*` parameters.

## 2. Attribute Storage Scheme

### Complete Attribute Mapping Table

| Attribute Category       | Storage Location                                       | Applicable Objects | Parameter Passing Method              | Description                            |
| ------------------------ | ------------------------------------------------------ | ------------------ | ------------------------------------- | -------------------------------------- |
| **Object Type**          | `table.info['table_type']`                             | ALL                | `info={'table_type': 'VIEW'}`         | Core identifier (one of TABLE/VIEW/MV) |
| **Definition Statement** | `table.info['definition']`                             | VIEW, MV           | `info={'definition': 'SELECT ...'}`   | SELECT statement                       |
| **General Comment**      | `table.comment`                                        | ALL                | `comment='...'`                       | SQLAlchemy standard attribute          |
| **Partitioning**         | `table.dialect_options['starrocks']['partition_by']`   | TABLE, MV          | `starrocks_partition_by='...'`        | Passed via kwargs                      |
| **Distribution**         | `table.dialect_options['starrocks']['distributed_by']` | TABLE, MV          | `starrocks_distributed_by='...'`      | Passed via kwargs                      |
| **Ordering Key**         | `table.dialect_options['starrocks']['order_by']`       | TABLE, MV          | `starrocks_order_by='...'`            | Passed via kwargs                      |
| **Refresh**              | `table.dialect_options['starrocks']['refresh']`        | MV                 | `starrocks_refresh='IMMEDIATE ASYNC'` | Passed via kwargs                      |
| **Properties**           | `table.dialect_options['starrocks']['properties']`     | TABLE, MV          | `starrocks_properties={...}`          | Passed via kwargs                      |
| **Security Type**        | `table.dialect_options['starrocks']['security']`       | VIEW               | `starrocks_security='DEFINER'`        | Passed via kwargs                      |

### Why this Categorization?

- **`table.info`**: SQLAlchemy's native metadata dictionary, suitable for storing cross-dialect general information (e.g., `table_type`, `definition`).
- **`table.comment`**: A standard SQLAlchemy attribute, supported by most databases, so the standard attribute is used directly.
- **`table.dialect_options['starrocks']`**: Passed via `starrocks_*` parameters, which `Table` automatically parses and stores.

**Specifics**:

1. **`info["table_type"]` (representing the actual type)**:
   - Definition: A string stored in `table.info["table_type"]`.
   - Purpose: Identifies the **actual type** of the current object.
   - Possible values: One of `"TABLE"` (Table), `"VIEW"` (View), `"MATERIALIZED_VIEW"` (Materialized View) (mutually exclusive).
   - Default value: If `None` or empty, defaults to `"TABLE"`.

## 3. Implementation Plans for Each Layer

### 3.1 Schema Layer (`schema.py`)

#### Approach A: `View`/`MaterializedView` Inherits from `Table` ⭐ **Adopted**

**Implementation Idea**: Let `Table`'s `__init__` automatically handle all parameters.

```python
class View(Table):
    def __init__(self, name: str, definition: str,
                 metadata: Optional[MetaData] = None, **kwargs):
        """
        Directly leverage Table's parameter handling mechanism.
        All starrocks_* parameters are passed via kwargs, and Table will handle them automatically.
        """
        # Set info
        info = kwargs.setdefault('info', {})
        info['table_type'] = 'VIEW'
        info['definition'] = definition

        # Call Table.__init__, which automatically handles comment and starrocks_* parameters
        super().__init__(name, metadata, **kwargs)

    # Convenience properties: get values from Table via @property
    @property
    def definition(self):
        return self.info.get('definition')

    @property
    def security(self):
        return self.dialect_options.get('starrocks', {}).get('security')


class MaterializedView(Table):
    def __init__(self, name: str, definition: str,
                 metadata: Optional[MetaData] = None, **kwargs):
        info = kwargs.setdefault('info', {})
        info['table_type'] = 'MATERIALIZED_VIEW'
        info['definition'] = definition

        super().__init__(name, metadata, **kwargs)

    @property
    def definition(self):
        return self.info.get('definition')

    @property
    def partition_by(self):
        return self.dialect_options.get('starrocks', {}).get('partition_by')

    @property
    def refresh(self):
        return self.dialect_options.get('starrocks', {}).get('refresh')

    # ... Other properties similar
```

**Pros**:

- Fully utilizes Table's parameter handling mechanism, resulting in concise code.
- Users can mix positional arguments and `starrocks_*` arguments.
- Consistent with SQLAlchemy's design patterns.
- Reflection, Comparison, and other mechanisms are naturally supported.

**Cons**:

- Requires understanding Table's parameter handling mechanism.
- Parameter names need to be prefixed with `starrocks_` (but this is standard SQLAlchemy practice).

#### Approach B: `View`/`MaterializedView` Inherits from `SchemaItem` ❌ **Not Adopted**

**Pros**:

- Lighter-weight, clearly distinguished from Table.
- Does not depend on Table's complex mechanisms.

**Cons**:

- Cannot leverage Table's infrastructure (columns, reflection, comparison, etc.).
- Requires significant custom logic for integration.

### 3.2 Reflection Layer (`reflection.py` and `dialect.py`)

#### Core Strategy

**Why must `_setup_parser` and `reflect_table` be overridden?**

1. **`_setup_parser`**: The core method of MySQL Dialect (with `@reflection.cache`), responsible for parsing Table attributes (using `SHOW CREATE TABLE` in MySQL) and returning `ReflectedState`.
2. **`reflect_table`**: SQLAlchemy's standard reflection entry point, `Table.autoload_with=engine` will eventually call it.

**Problem**: The standard process only knows how to reflect TABLEs, not how to obtain specific attributes of VIEWs/MVs (e.g., definition).

**Solution**:

1. **Override `_setup_parser`**: Return different `ReflectedState` subclasses based on the object type.
   - Query `table_type` (only once, leveraging `@reflection.cache`).
   - Dispatch to `_setup_table_parser`, `_setup_view_parser`, `_setup_mv_parser`.
   - Return `ReflectedState`, `ReflectedViewState`, `ReflectedMVState` respectively.
2. **Override `reflect_table`**: Supplement VIEW/MV specific attributes.
   - Call the parent class to complete standard reflection (columns, constraints, etc.).
   - Get `table_type` from the cached `ReflectedState`.
   - Set `table.info['table_type']`.

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
    def table_type(self) -> str:
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
    def table_type(self) -> str:
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
    def table_type(self) -> str:
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

        Key: Query table_type only once here, leveraging @reflection.cache.
        """
        # 1. Query object type (only once)
        table_type = self._get_table_type_from_db(connection, table_name, schema)

        # 2. Dispatch based on type
        if table_type == 'VIEW':
            return self._setup_view_parser(connection, table_name, schema, info_cache)
        elif table_type == 'MATERIALIZED_VIEW':
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

    def _get_table_type_from_db(self, connection, table_name, schema) -> str:
        """
        Query object type from the database (without cache).
        Only called once in _setup_parser.
        """
        # 1. Query information_schema.tables
        # 2. If it's a VIEW, further query materialized_views to distinguish
        # 3. Return "TABLE" / "VIEW" / "MATERIALIZED_VIEW"
        # ...
```

**Step 3: Provide `get_table_type` Method**

```python
# dialect.py

class StarRocksDialect(MySQLDialect_pymysql):

    @reflection.cache
    def get_table_type(self, connection, table_name, schema=None) -> str:
        """
        Get the object's table_type (from cache).

        Reuse _setup_parser's cache via _parsed_state_or_create.
        """
        parsed_state = self._parsed_state_or_create(connection, table_name, schema)
        return parsed_state.table_type  # Use @property
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
        2. Get table_type from cache
        3. Set table.info['table_type']
        4. Set VIEW/MV specific attributes
        """
        # 1. Call parent class (will call get_pk_constraints, etc., which will trigger _setup_parser)
        super().reflect_table(table, table_name, schema, **kwargs)

        # 2. Get table_type and parsed_state (from cache)
        parsed_state = self.dialect._parsed_state_or_create(self.bind, table_name, schema)
        table_type = parsed_state.table_type

        # 3. Set info['table_type']
        table.info['table_type'] = table_type

        # 4. Set specific attributes based on type
        if table_type == 'VIEW':
            self._reflect_view_attributes(table, parsed_state)
        elif table_type == 'MATERIALIZED_VIEW':
            self._reflect_mv_attributes(table, parsed_state)

    def _reflect_view_attributes(self, table, view_state):
        """Set View specific attributes from ReflectedViewState"""
        table.info['definition'] = view_state.definition
        if view_state.security:
            table.dialect_options.setdefault('starrocks', {})['security'] = view_state.security

    def _reflect_mv_attributes(self, table, mv_state):
        """Set MV specific attributes from ReflectedMVState"""
        table.info['definition'] = mv_state.definition

        opts = table.dialect_options.setdefault('starrocks', {})
        if mv_state.partition_info:
            opts['partition_by'] = ...  # Extract from partition_info
        if mv_state.distribution_info:
            opts['distributed_by'] = ...
        if mv_state.refresh_info:
            opts['refresh'] = mv_state.refresh_info
        # ... Other attributes
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

**Idea**: Override methods like `get_pk_constraints()`, query `table_type` internally, and return the corresponding result.

**Cons**:

- Code redundancy: requires overriding multiple `get_*` methods
- Not elegant: does not leverage OOP polymorphism
- Same performance: still requires one DB query + caching

#### Approach C: Independent reflect_view and reflect_materialized_view ❌ **Not Adopted**

**Cons**:

- **Cannot be Integrated with `Table.autoload_with=engine`** (Fatal Flaw)
- Users need to call different methods based on type.

#### How to Distinguish VIEW and MV

In StarRocks' `information_schema.tables`, both VIEW and MV have `'VIEW'` for `table_type`. They need to be distinguished by `information_schema.materialized_views`:

```python
def _get_table_type_from_db(self, connection, table_name, schema) -> str:
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

### 3.3 Compiler Layer (`dialect.py` - DDLCompiler)

#### Approach A: Extend `visit_create_table` + Reuse Independent Visit Methods ⭐ **Adopted**

```python
class StarRocksDDLCompiler(MySQLDDLCompiler):

    def visit_create_table(self, create: CreateTable, **kw: Any) -> str:
        """Check table_type, dispatch to corresponding method"""
        table: Table = create.element
        table_type: str = table.info.get('table_type', 'TABLE')

        if table_type == 'VIEW':
            return self._compile_create_view_from_table(table, create, **kw)
        elif table_type == 'MATERIALIZED_VIEW':
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
- `Table(info={'table_type': 'VIEW'})` cannot work.

### 3.4 Autogenerate/Compare Layer (`compare.py`)

#### Approach A: Maintain Separation, Share Comparison Logic ⭐ **Adopted**

**Reason**:

1. Alembic's `_autogen_for_tables` only calls `get_table_names()`.
2. Different default values and comparison rules for TABLE/VIEW/MV.
3. Need separate entry points to trigger their respective comparisons.

```python
@comparators_dispatch_for_starrocks("schema")
def autogen_for_views(autogen_context, upgrade_ops, schemas):
    """Specifically handle views, compare using Table objects"""
    for schema in schemas:
        conn_views = {name: inspector.reflect_table(name, schema)
                     for name in inspector.get_view_names(schema)}
        meta_views = {t.name: t for t in metadata.tables.values()
                     if t.info.get('table_type') == 'VIEW'}

        _compare_objects(conn_views, meta_views, 'VIEW', ...)

@comparators_dispatch_for_starrocks("schema")
def autogen_for_mvs(autogen_context, upgrade_ops, schemas):
    """Specifically handle MVs"""
    # Similar logic...

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

### 3.5 Operations Layer (`ops.py`)

#### Approach A: Independent Ops + from_table() Method ⭐ **Adopted**

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

#### Approach B: Extend `CreateTableOp` ❌ **Not Adopted**

**Cons**:

- `CreateTableOp` semantics are unclear.
- Does not align with Alembic's design.

### 3.6 Render Layer (`render.py`)

Maintain existing logic, render as dedicated operations:

- `CreateViewOp` → `op.create_view(...)`
- `CreateMaterializedViewOp` → `op.create_materialized_view(...)`

Both ways users write work:

```python
# Recommended way
op.create_view('my_view', 'SELECT ...', starrocks_security='DEFINER')

# Also works
op.create_table('my_view',
                info={'table_type': 'VIEW', 'definition': 'SELECT ...'},
                starrocks_security='DEFINER')
```

## 4. Detailed Design Points

### 4.1 Schema Layer Key Points

**Design Points**:

1. **Parameter Passing**: All `starrocks_*` parameters are passed to `Table.__init__` via `kwargs`.
2. **Info Setting**: Set `info` before calling `super().__init__`.
3. **@property Access**: Provide convenient properties to get values from Table.

**Example**:

```python
# User creation
my_view = View('v1', 'SELECT * FROM t1',
               metadata=metadata,
               comment='My view',
               starrocks_security='DEFINER')

# Equivalent to
my_view = Table('v1', metadata,
                info={'table_type': 'VIEW', 'definition': 'SELECT * FROM t1'},
                comment='My view',
                starrocks_security='DEFINER')
```

### 4.2 Reflection Layer Key Points

**Design Points**:

1. **`_setup_parser` is Core**:

   - With `@reflection.cache`, ensures performance.
   - Returns different `ReflectedState` subclasses based on `table_type`.
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
                       ├─ _get_table_type_from_db() → "VIEW"
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
       get_table_type('my_view')  # Get from cached state
           ↓
        Set table.info['table_type']
   ```

4. **Column Information Acquisition**: TODO - Not Implemented Yet (Can be implemented in `_setup_view_parser` and `_setup_mv_parser`)

### 4.3 Compiler Layer Key Points

**Design Points**:

1. **`visit_create_table` Extension**: Check `table_type`, dispatch to corresponding method.
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

### 4.4 Compare Layer Key Points

**Design Points**:

1. **Separate Entry Points**: TABLE/VIEW/MV maintain independent `autogen_for_*` entry points.
2. **Uniform Object Type**: All are Table objects, but `table_type` is different.
3. **Shared Comparison Logic**: Extract common functions for comparison.
4. **Default Value Handling**: Use different default values for different object types.
5. **Attribute Normalization**: Normalize attributes before comparison (e.g., SQL formatting).

**Key Logic**:

```python
def _compare_objects(conn_objs, meta_objs, object_kind, ...):
    # Create
    for name in (meta_objs.keys() - conn_objs.keys()):
        upgrade_ops.append(CreateXxxOp.from_table(meta_objs[name]))

    # Drop
    for name in (conn_objs.keys() - meta_objs.keys()):
        upgrade_ops.append(DropXxxOp.from_table(conn_objs[name]))

    # Alter
    for name in (conn_objs.keys() & meta_objs.keys()):
        conn_obj = conn_objs[name]
        meta_obj = meta_objs[name]

        # Compare various attributes
        if _compare_attr(conn_obj.info['definition'],
                        meta_obj.info['definition']):
            # Generate AlterOp
```

### 4.5 Operations Layer Key Points

**Design Points**:

1. **Maintain Independent Ops**: Do not modify `CreateTableOp`.
2. **from_table() Method**: Construct Op from Table object.
3. **Double Construction**: Support traditional parameters and table parameters.
4. **Attribute Extraction**: Extract from `table.info` and `table.dialect_options`.
5. **Validation**: Check if `table_type` matches.

### 4.6 Metadata Management Key Points

**Design Points**:

1. **Uniform Interface**: Use `metadata.tables` uniformly.
2. **Filter Logic**: Objects can be filtered by `table_type`.

## 5. Implementation Roadmap

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

1. Modify `autogen_for_views` to use Table objects.
2. Modify `autogen_for_mvs` to use Table objects.
3. Implement `_compare_objects` unified logic.
4. Test: autogenerate detects changes.

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

## 6. Key Considerations

### 6.1 refresh Parameter Handling

**User Input**:

```python
# Do not subdivide moment and type in refresh, pass it as a single string
MaterializedView('mv1', 'SELECT ...',
                starrocks_refresh='IMMEDIATE ASYNC')
```

**Internal Storage**: Unified as `dialect_options['starrocks']['refresh'] = 'IMMEDIATE ASYNC'`.

**Compile-time Parsing**: `'IMMEDIATE ASYNC'` → `REFRESH IMMEDIATE ASYNC`.

### 6.2 Columns Support

**Current**: TODO - Not Implemented

- View/MV columns are determined by the SELECT statement.
- Can be obtained via reflection (TODO).

**Future**: Consider supporting explicit column definitions (for type hints).

### 6.3 Default Value Handling

- `table_type` in `info` defaults to `'TABLE'`.
- `table_type` is automatically set during reflection.
- Pay attention to default value differences for different object types during comparison.

### 6.4 Performance Considerations

- `_setup_parser()` uses `@reflection.cache` to cache results.
- The same `table_name` is queried only once during a single reflection.
- `get_table_type()` reuses the cache via `_parsed_state_or_create()`.

### 6.5 Error Handling

- Missing `definition` → `CompileError` during compilation.
- Incorrect `table_type` → Raise ValueError exception.
- Reflection failure → Return `None` or raise an exception.

## 7. Example Code

### 7.1 User-Defined View/MV

**Convenience Class Method**:

```python
from sqlalchemy import MetaData
from starrocks import View, MaterializedView

metadata = MetaData()

# View
my_view = View(
    'user_view',
    'SELECT id, name FROM users WHERE active = 1',
    metadata=metadata,
    schema='test',
    comment='Active users',
    starrocks_security='DEFINER'
)

# MV
my_mv = MaterializedView(
    'user_stats_mv',
    'SELECT date, COUNT(*) FROM logs GROUP BY date',
    metadata=metadata,
    schema='test',
    comment='Daily stats',
    starrocks_partition_by='RANGE(date)',
    starrocks_distributed_by='HASH(date) BUCKETS 8',
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
    schema='test',
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
from sqlalchemy import Integer, String
from starrocks import View

Base = declarative_base()

# Method 1: Directly define View table
class UserView(Base):
    __tablename__ = 'user_view'
    __table_args__ = {
        'info': {
            'table_type': 'VIEW',
            'definition': 'SELECT id, name FROM users WHERE active = 1'
        },
        'starrocks_security': 'DEFINER'
    }

    # Define columns (optional, for type hints)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50))

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

### 7.2 Reflection

```python
from sqlalchemy import create_engine, MetaData, Table

engine = create_engine('starrocks://...')
metadata = MetaData()

# Autoload - Automatically recognize object type
my_table = Table('my_view', metadata, autoload_with=engine, schema='test')
print(my_table.info['table_type'])  # 'VIEW'
print(my_table.info['definition'])    # 'SELECT ...'
print(my_table.dialect_options['starrocks']['security'])  # 'DEFINER'
```

### 7.3 Autogenerate

**Migration generated after detecting changes**:

```python
def upgrade():
    op.create_view(
        'user_view',
        'SELECT id, name FROM users WHERE active = 1',
        schema='test',
        comment='Active users',
        starrocks_security='DEFINER'
    )

    op.create_materialized_view(
        'user_stats_mv',
        'SELECT date, COUNT(*) FROM logs GROUP BY date',
        schema='test',
        comment='Daily stats',
        starrocks_partition_by='RANGE(date)',
        starrocks_distributed_by='HASH(date) BUCKETS 8',
        starrocks_refresh='IMMEDIATE ASYNC'
    )
```

## 8. Summary

This design achieves:

1. **Unified Representation**: All objects are Tables, distinguished by `info["table_type"]`.
2. **Concise Implementation**: Fully leverages Table's parameter handling mechanism.
3. **Flexible Interface**: Supports multiple definition methods (convenience class, Table, ORM).
4. **Clear Layering**: info (general) / comment (standard) / dialect_options (dialect-specific).
5. **Core Overrides**: `reflect_table` is key, supports autoload.
6. **Separated Comparison**: TABLE/VIEW/MV independent entry points, shared core logic.

This architecture maintains user interface friendliness while fully utilizing SQLAlchemy's infrastructure, providing a solid foundation for future extensions.
