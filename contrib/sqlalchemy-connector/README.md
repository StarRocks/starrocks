### Superset usage


Install(If you install `superset` with Docker, install it with `root`)：
```
pip install .
```

Uninstall
```
pip uninstall sqlalchemy_starrocks
```

In superset, use `Other` database, and set url is：
```
starrocks://root:@x.x.x.x:9030/superset_db
```
