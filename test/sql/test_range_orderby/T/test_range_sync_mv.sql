-- name: test_range_sync_mv @cloud
-- Test Point: History and concurrent loads preserve every supported sync MV shape, sibling indexes and lifecycle.
-- Method: Compare base, rewritten and direct sync-index data; assert FINISHED jobs and real layouts.
-- Scope: Shared-data range synchronous MV backfill, double writes and maintenance.

CREATE DATABASE db_${uuid0};

USE db_${uuid0};

shell: exec python3 -c 'exec('"'"'import os,signal\nsignal.alarm(5)\ndirectory=\'"'"'/tmp/range_sync_mv_test_range_sync_mv_${uuid0}\'"'"'\nos.mkdir(directory,0o700)\nhelper_source=\'"'"'import concurrent.futures\\nimport contextlib\\nimport ctypes\\nimport io\\nimport json\\nimport os\\nfrom pathlib import Path\\nimport signal\\nimport subprocess\\nimport sys\\nimport time\\nfrom urllib.parse import urljoin, urlparse\\ndb = "db_${uuid0}"\\ncfg = None\\nconn = None\\nstage_deadline = 0\\ndef remaining():\\n    seconds = stage_deadline - time.monotonic()\\n    assert seconds > 0, "total stage deadline expired"\\n    return seconds\\ndef connection(database=db):\\n    return pymysql.connect(host=cfg.mysql_host, port=int(cfg.mysql_port), user=cfg.mysql_user,\\n                           password=cfg.mysql_password, database=database, autocommit=True,\\n                           connect_timeout=min(5, remaining()), read_timeout=min(15, remaining()),\\n                           write_timeout=min(15, remaining()))\\ndef q(sql, c=None):\\n    c = conn if c is None else c\\n    c._read_timeout = min(15, remaining())\\n    c._write_timeout = min(15, remaining())\\n    with c.cursor() as cursor:\\n        cursor.execute(sql)\\n        return cursor.fetchall()\\ndef records(sql):\\n    conn._read_timeout = min(15, remaining())\\n    conn._write_timeout = min(15, remaining())\\n    with conn.cursor(pymysql.cursors.DictCursor) as cursor:\\n        cursor.execute(sql)\\n        return cursor.fetchall()\\ndef load(row, label):\\n    url = "http://" + cfg.mysql_host + ":" + str(cfg.http_port) + "/api/" + db + "/t/_stream_load"\\n    endpoints = {(str(r[1]).lower(), int(r[4])) for sql in ("SHOW BACKENDS", "SHOW COMPUTE NODES") for r in q(sql)}\\n    endpoints.add((cfg.mysql_host.lower(), int(cfg.http_port)))\\n    headers = {"label": db + label, "column_separator": ",", "columns": "id,g,v,n,filter_col",\\n               "strict_mode": "true", "max_filter_ratio": "0", "Expect": "100-continue"}\\n    with requests.Session() as session:\\n        session.trust_env = False\\n        for redirect in range(6):\\n            result = session.put(url, data=row.encode(), headers=headers,\\n                                 auth=(cfg.mysql_user, cfg.mysql_password),\\n                                 timeout=(min(5, remaining()), min(15, remaining())), allow_redirects=False)\\n            if result.status_code not in (301, 302, 303, 307, 308):\\n                result.raise_for_status()\\n                response = result.json()\\n                assert response["Status"] == "Success", response\\n                assert response["NumberTotalRows"] == response["NumberLoadedRows"] == 1, response\\n                assert response["NumberFilteredRows"] == 0, response\\n                return "Success 1 0"\\n            assert redirect < 5, "stream-load redirect limit exceeded"\\n            url = urljoin(url, result.headers["Location"])\\n            target = urlparse(url)\\n            assert target.scheme in ("http", "https") and target.username is None and target.password is None\\n            assert (target.hostname.lower(), target.port or (443 if target.scheme == "https" else 80)) in endpoints, (\\n                "stream-load redirect is outside the configured cluster")\\ndef latest(mv):\\n    rows = records("SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC")\\n    return next(r for r in rows if r["RollupIndexName"] == mv)\\ndef assert_creation_overlap(job_id, job):\\n    assert job["JobId"] == job_id and job["State"] not in ("FINISHED", "CANCELLED"), (\\n        "successful writers did not overlap the same unfinished creation job", job_id, job)\\ndef supervise(argv, budget=105):\\n    # The wrapper is exec\\\'"'"'ed over the harness shell. Its budget plus this five-second\\n    # descendant shutdown is shorter than the harness\\\'"'"'s 120-second timeout.\\n    libc = ctypes.CDLL(None, use_errno=True)\\n    assert libc.prctl(36, 1, 0, 0, 0) == 0, "cannot enable Linux child subreaping"\\n    child = subprocess.Popen(argv, start_new_session=True)\\n    timed_out = False\\n    try:\\n        try:\\n            status = child.wait(timeout=budget)\\n        except subprocess.TimeoutExpired:\\n            timed_out = True\\n            status = None\\n    finally:\\n        shutdown_deadline = time.monotonic() + 5\\n        for sig in (signal.SIGTERM, signal.SIGKILL):\\n            try:\\n                os.killpg(child.pid, sig)\\n            except ProcessLookupError:\\n                pass\\n            try:\\n                child.wait(timeout=min(2, max(0.01, shutdown_deadline - time.monotonic())))\\n            except subprocess.TimeoutExpired:\\n                if sig == signal.SIGKILL:\\n                    raise\\n        # Subreaping lets the supervisor join grandchildren even when their leader\\n        # was killed while its ThreadPoolExecutor or HTTP request was still running.\\n        while True:\\n            try:\\n                pid, _ = os.waitpid(-child.pid, os.WNOHANG)\\n            except ChildProcessError:\\n                break\\n            if pid == 0:\\n                assert time.monotonic() < shutdown_deadline, "descendants did not terminate and join"\\n                time.sleep(0.01)\\n    assert not timed_out, "total stage timeout: process group terminated and joined"\\n    assert status == 0, "stage failed: process group terminated and joined, status=" + str(status)\\ndef worker(code, cleanup=False):\\n    global cfg, conn, pymysql, requests, stage_deadline\\n    import pymysql\\n    import requests\\n    sys.path.insert(0, os.getcwd())\\n    from lib.sr_sql_lib import StarrocksSQLApiLib\\n    stage_deadline = time.monotonic() + 100\\n    with contextlib.redirect_stdout(io.StringIO()):\\n        cfg = StarrocksSQLApiLib()\\n    conn = connection(database=None if cleanup else db)\\n    cfg.mysql_lib.connector = conn\\n    try:\\n        exec(code, globals())\\n    finally:\\n        if conn.open:\\n            conn.close()\\ndef remove_helper():\\n    directory = Path(__file__).parent\\n    assert directory.name.startswith("range_sync_mv_"), directory\\n    assert directory.stat().st_uid == os.getuid() and not directory.is_symlink(), directory\\n    Path(__file__).unlink()\\n    directory.rmdir()\\n    print("helper removed")\\nstate_path = Path(__file__).parent / "config.json"\\nHAS_CONFIG = False\\nif __name__ == "__main__":\\n    if sys.argv[1] == "--worker":\\n        worker(sys.argv[2].replace("__NL__", "\\\\n"), cleanup=len(sys.argv) == 4)\\n    elif sys.argv[1] == "--cleanup":\\n        if HAS_CONFIG and state_path.exists():\\n            supervise([sys.executable, __file__, "--worker", "restore_config()", "--cleanup"])\\n        remove_helper()\\n    else:\\n        supervise([sys.executable, __file__, "--worker", sys.argv[1]])\\n\'"'"'\nfd=os.open(directory+"/helper.py",os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)\nwith os.fdopen(fd,"w") as out:\n    out.write(helper_source)\nprint("helper installed")\n'"'"')'

CLEANUP {
    shell: exec python3 -c 'exec('"'"'import os,sys\nfrom pathlib import Path\np=Path(\'"'"'/tmp/range_sync_mv_test_range_sync_mv_${uuid0}/helper.py\'"'"')\nif p.exists():\n    os.execv(sys.executable, [sys.executable,str(p),"--cleanup"])\n'"'"')'
    DROP DATABASE IF EXISTS db_${uuid0} FORCE;
} END CLEANUP

SET enable_range_distribution = true;

-- Reject generated-name collisions before creating a job; a real mv_ column remains selectable.

shell: exec python3 '/tmp/range_sync_mv_test_range_sync_mv_${uuid0}/helper.py' 'q("SET enable_range_distribution = true")__NL__q("CREATE TABLE collision_source (id INT, g BIGINT, v BIGINT, mv_sum_v BIGINT, mv_count_v BIGINT, mv_shifted BIGINT, mv_expr BIGINT) DUPLICATE KEY(id) ORDER BY(id) PROPERTIES(\"replication_num\"=\"1\")")__NL__q("INSERT INTO collision_source VALUES (1,1,10,100,100,100,100)")__NL__queries = ["SELECT g,SUM(v) FROM collision_source GROUP BY g", "SELECT g,COUNT(v) FROM collision_source GROUP BY g", "SELECT g+10 AS shifted,v FROM collision_source", "SELECT g,SUM(v+1) AS expr FROM collision_source GROUP BY g", "SELECT g,SUM(V) FROM collision_source GROUP BY g", "SELECT g+1 AS fresh_key,SUM(v) FROM collision_source GROUP BY g+1"]__NL__for i, query in enumerate(queries):__NL__    try:__NL__        q("CREATE MATERIALIZED VIEW rejected_collision_" + str(i) + " AS " + query)__NL__    except pymysql.MySQLError as error:__NL__        assert "conflicts with base column" in str(error), error__NL__    else:__NL__        raise AssertionError("generated-name collision was accepted: " + query)__NL__assert not any(row["RollupIndexName"].startswith("rejected_collision_") for row in records("SHOW ALTER MATERIALIZED VIEW"))__NL__q("CREATE MATERIALIZED VIEW mv_collision_direct AS SELECT g,mv_sum_v FROM collision_source")__NL__while True:__NL__    job = latest("mv_collision_direct")__NL__    assert job["State"] != "CANCELLED", job__NL__    if job["State"] == "FINISHED":__NL__        break__NL__    time.sleep(0.2)__NL__q("INSERT INTO collision_source VALUES (2,1,20,200,200,200,200)")__NL__assert q("SELECT g,mv_sum_v FROM mv_collision_direct [_SYNC_MV_] ORDER BY g,mv_sum_v") == ((1,100),(1,200))__NL__assert q("SELECT g,SUM(v) FROM collision_source GROUP BY g") == ((1,30),)__NL__q("DROP MATERIALIZED VIEW mv_collision_direct")__NL__q("CREATE MATERIALIZED VIEW mv_drop_guard AS SELECT g+10 AS kept_key,v FROM collision_source")__NL__while True:__NL__    job = latest("mv_drop_guard")__NL__    assert job["State"] != "CANCELLED", job__NL__    if job["State"] == "FINISHED":__NL__        break__NL__    time.sleep(0.2)__NL__try:__NL__    q("ALTER TABLE collision_source DROP COLUMN G")__NL__except pymysql.MySQLError as error:__NL__    assert "rollup" in str(error) and "define expr" in str(error), error__NL__else:__NL__    raise AssertionError("computed MV source column was dropped")__NL__assert q("SELECT mv_kept_key,v FROM mv_drop_guard [_SYNC_MV_] ORDER BY mv_kept_key,v") == ((11,10),(11,20))__NL__q("DROP MATERIALIZED VIEW mv_drop_guard")__NL__q("ALTER TABLE collision_source DROP COLUMN g")__NL__assert q("SELECT id,v,mv_sum_v FROM collision_source ORDER BY id") == ((1,10,100),(2,20,200))__NL__q("DROP TABLE collision_source")__NL__q("CREATE TABLE fields_source (id INT,v BIGINT,s STRUCT<b INT,a INT>) DUPLICATE KEY(id) ORDER BY(id) PROPERTIES(\"replication_num\"=\"1\",\"cloud_native_fast_schema_evolution_v2\"=\"true\")")__NL__q("INSERT INTO fields_source VALUES (1,2,named_struct('"'"'b'"'"',4,'"'"'a'"'"',3))")__NL__q("CREATE MATERIALIZED VIEW mv_fields_source AS SELECT s.a+1 AS projected,v FROM fields_source")__NL__while True:__NL__    job = latest("mv_fields_source")__NL__    assert job["State"] != "CANCELLED", job__NL__    if job["State"] == "FINISHED":__NL__        break__NL__    time.sleep(0.2)__NL__for clause in ["MODIFY COLUMN s DROP FIELD a", "DROP COLUMN s", "RENAME COLUMN s TO s2"]:__NL__    try:__NL__        q("ALTER TABLE fields_source " + clause)__NL__    except pymysql.MySQLError as error:__NL__        assert "rollup" in str(error), error__NL__    else:__NL__        raise AssertionError("struct dependency was orphaned: " + clause)__NL__assert q("SELECT mv_projected,v FROM mv_fields_source [_SYNC_MV_]") == ((4,2),)__NL__q("ALTER TABLE fields_source MODIFY COLUMN s DROP FIELD b")__NL__q("INSERT INTO fields_source VALUES (2,4,named_struct('"'"'a'"'"',5))")__NL__q("ALTER TABLE fields_source MODIFY COLUMN s ADD FIELD c INT")__NL__q("INSERT INTO fields_source VALUES (3,6,named_struct('"'"'a'"'"',7,'"'"'c'"'"',9))")__NL__assert q("SELECT mv_projected,v FROM mv_fields_source [_SYNC_MV_] ORDER BY mv_projected") == ((4,2),(6,4),(8,6))__NL__assert q("SELECT id,s.a FROM fields_source ORDER BY id") == ((1,3),(2,5),(3,7))__NL__q("DROP MATERIALIZED VIEW mv_fields_source")__NL__q("DROP TABLE fields_source")__NL__q("CREATE TABLE state_source (id INT,g INT,s STRUCT<a INT,b INT> AS named_struct('"'"'a'"'"',id,'"'"'b'"'"',id)) DUPLICATE KEY(id) ORDER BY(id) PROPERTIES(\"replication_num\"=\"1\")")__NL__q("INSERT INTO state_source (id,g) VALUES (1,1),(3,1)")__NL__q("CREATE MATERIALIZED VIEW mv_state_source AS SELECT g,AVG_UNION(AVG_STATE(s.a)) AS av FROM state_source GROUP BY g")__NL__while True:__NL__    job = latest("mv_state_source")__NL__    assert job["State"] != "CANCELLED", job__NL__    if job["State"] == "FINISHED":__NL__        break__NL__    time.sleep(0.2)__NL__try:__NL__    q("ALTER TABLE state_source MODIFY COLUMN s STRUCT<a DECIMAL(18,4),b INT> AS named_struct('"'"'a'"'"',CAST(id AS DECIMAL(18,4)),'"'"'b'"'"',id)")__NL__except pymysql.MySQLError as error:__NL__    assert "state" in str(error) and "rollup" in str(error), error__NL__else:__NL__    raise AssertionError("incompatible aggregate state was admitted")__NL__assert q("SELECT g,CAST(AVG_MERGE(mv_av) AS BIGINT) FROM mv_state_source [_SYNC_MV_] GROUP BY g") == ((1,2),)__NL__q("INSERT INTO state_source (id,g) VALUES (5,1)")__NL__assert q("SELECT g,CAST(AVG_MERGE(mv_av) AS BIGINT) FROM mv_state_source [_SYNC_MV_] GROUP BY g") == ((1,3),)__NL__q("DROP MATERIALIZED VIEW mv_state_source")__NL__q("DROP TABLE state_source")__NL__q("CREATE TABLE struct_predicate_source (id INT,v BIGINT,s STRUCT<unused INT,b STRUCT<c INT>,a INT>) DUPLICATE KEY(id) ORDER BY(id) PROPERTIES(\"replication_num\"=\"1\",\"cloud_native_fast_schema_evolution_v2\"=\"true\")")__NL__q("INSERT INTO struct_predicate_source VALUES (1,10,named_struct('"'"'unused'"'"',9,'"'"'b'"'"',named_struct('"'"'c'"'"',2),'"'"'a'"'"',1)),(2,20,NULL)")__NL__q("CREATE MATERIALIZED VIEW mv_struct_predicate AS SELECT id,v FROM struct_predicate_source WHERE s.a>0 AND s.b.c>1")__NL__while True:__NL__    job = latest("mv_struct_predicate")__NL__    assert job["State"] != "CANCELLED", job__NL__    if job["State"] == "FINISHED":__NL__        break__NL__    time.sleep(0.2)__NL__assert q("SELECT id,v FROM mv_struct_predicate [_SYNC_MV_] ORDER BY id") == ((1,10),)__NL__q("ALTER TABLE struct_predicate_source MODIFY COLUMN s DROP FIELD unused")__NL__q("INSERT INTO struct_predicate_source VALUES (3,30,named_struct('"'"'b'"'"',named_struct('"'"'c'"'"',3),'"'"'a'"'"',2)),(4,40,named_struct('"'"'b'"'"',named_struct('"'"'c'"'"',4),'"'"'a'"'"',0))")__NL__def load_struct_predicate(row, label):__NL__    url = "http://" + cfg.mysql_host + ":" + str(cfg.http_port) + "/api/" + db + "/struct_predicate_source/_stream_load"__NL__    endpoints = {(str(r[1]).lower(), int(r[4])) for sql in ("SHOW BACKENDS", "SHOW COMPUTE NODES") for r in q(sql)}__NL__    endpoints.add((cfg.mysql_host.lower(), int(cfg.http_port)))__NL__    headers = {"label": db + label, "format": "json", "strip_outer_array": "true", "columns": "id,v,s",__NL__               "strict_mode": "true", "max_filter_ratio": "0", "Expect": "100-continue"}__NL__    with requests.Session() as session:__NL__        session.trust_env = False__NL__        for redirect in range(6):__NL__            result = session.put(url, data=json.dumps([row]).encode(), headers=headers,__NL__                                 auth=(cfg.mysql_user, cfg.mysql_password),__NL__                                 timeout=(min(5, remaining()), min(15, remaining())), allow_redirects=False)__NL__            if result.status_code not in (301, 302, 303, 307, 308):__NL__                result.raise_for_status()__NL__                response = result.json()__NL__                assert response["Status"] == "Success", response__NL__                assert response["NumberTotalRows"] == response["NumberLoadedRows"] == 1, response__NL__                assert response["NumberFilteredRows"] == 0, response__NL__                return__NL__            assert redirect < 5, "stream-load redirect limit exceeded"__NL__            url = urljoin(url, result.headers["Location"])__NL__            target = urlparse(url)__NL__            assert target.scheme in ("http", "https") and target.username is None and target.password is None__NL__            assert (target.hostname.lower(), target.port or (443 if target.scheme == "https" else 80)) in endpoints, (__NL__                "stream-load redirect is outside the configured cluster")__NL__load_struct_predicate({"id":5,"v":50,"s":{"b":{"c":5},"a":3}}, "struct_where_match")__NL__load_struct_predicate({"id":6,"v":60,"s":{"b":{"c":6},"a":0}}, "struct_where_excluded")__NL__expected = ((1,10),(3,30),(5,50))__NL__q("SET enable_materialized_view_rewrite=false")__NL__q("SET enable_sync_materialized_view_rewrite=false")__NL__assert q("SELECT id,v FROM struct_predicate_source WHERE s.a>0 AND s.b.c>1 ORDER BY id") == expected__NL__assert q("SELECT id,v FROM mv_struct_predicate [_SYNC_MV_] ORDER BY id") == expected__NL__q("SET enable_materialized_view_rewrite=true")__NL__q("SET enable_sync_materialized_view_rewrite=true")__NL__assert q("SELECT id,v FROM struct_predicate_source WHERE s.a>0 AND s.b.c>1 ORDER BY id") == expected__NL__q("DROP MATERIALIZED VIEW mv_struct_predicate")__NL__q("DROP TABLE struct_predicate_source")__NL__print("6 collisions rejected; direct projection and source schema guards passed")__NL__'

CREATE TABLE t (id INT NOT NULL, g INT NOT NULL, v BIGINT, n BIGINT, filter_col INT)
DUPLICATE KEY(id)
PARTITION BY RANGE(id) (PARTITION p1 VALUES LESS THAN ("4"), PARTITION p2 VALUES LESS THAN MAXVALUE)
ORDER BY(id) PROPERTIES("replication_num" = "1");

INSERT INTO t VALUES (1,1,10,NULL,1), (2,1,20,2,1), (3,2,30,NULL,1), (4,2,40,4,-1), (5,3,50,5,1);

-- An excluded stream-loaded row proves loading before creation without changing the fixed filter oracle.

shell: exec python3 '/tmp/range_sync_mv_test_range_sync_mv_${uuid0}/helper.py' 'print(load("8,4,80,8,-1\n", "before"))__NL__'

SELECT g, SUM(v), COUNT(n) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

-- Launch and join both successful writers while the recorded creation job is WAITING_TXN or RUNNING; no detached process survives failure.

shell: exec python3 '/tmp/range_sync_mv_test_range_sync_mv_${uuid0}/helper.py' 'q("CREATE MATERIALIZED VIEW mv_sum AS SELECT g, SUM(v), COUNT(n) FROM t WHERE filter_col > 0 GROUP BY g")__NL__job = latest("mv_sum")__NL__deadline = time.monotonic() + 90__NL__while job["State"] not in ("WAITING_TXN", "RUNNING"):__NL__    assert job["State"] not in ("FINISHED", "CANCELLED"), job__NL__    assert time.monotonic() < deadline, job__NL__    time.sleep(0.1)__NL__    job = latest("mv_sum")__NL__job_id = job["JobId"]__NL__def insert():__NL__    c = connection()__NL__    try:__NL__        q("INSERT INTO t VALUES (6,1,5,NULL,1)", c)__NL__        return "INSERT success"__NL__    finally:__NL__        c.close()__NL__with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:__NL__    inserted = pool.submit(insert)__NL__    loaded = pool.submit(load, "7,2,7,7,1\n", "during")__NL__    assert inserted.result(timeout=70) == "INSERT success"__NL__    assert loaded.result(timeout=70) == "Success 1 0"__NL__    assert_creation_overlap(job_id, latest("mv_sum"))__NL__while True:__NL__    job = latest("mv_sum")__NL__    assert job["JobId"] == job_id and job["State"] != "CANCELLED", job__NL__    if job["State"] == "FINISHED":__NL__        break__NL__    assert time.monotonic() < deadline, job__NL__    time.sleep(0.1)__NL__cfg.wait_table_state_normal(db, "t", timeout_sec=10)__NL__assert q("SELECT COUNT(*) FROM t WHERE id IN (6,7)") == ((2,),)__NL__print("FINISHED INSERT success Success 1 0 joined overlap verified")__NL__'

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g, SUM(v), COUNT(n) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SELECT g, SUM(mv_sum_v), SUM(mv_count_n) FROM mv_sum [_SYNC_MV_] GROUP BY g ORDER BY g;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_sum";

function: check_hit_materialized_view('SELECT g, SUM(v), COUNT(n) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g', 'mv_sum')

SELECT g, SUM(v), COUNT(n) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SET query_including_mv_names = "";

-- Create siblings sequentially, including ordinary ADD ROLLUP derived from the base index.

CREATE MATERIALIZED VIEW mv_projection AS SELECT g, v FROM t WHERE filter_col > 0;

function: wait_materialized_view_finish()

CREATE MATERIALIZED VIEW mv_computed AS SELECT g + 10 AS shifted_g, v FROM t WHERE filter_col > 0;

function: wait_materialized_view_finish()

CREATE MATERIALIZED VIEW mv_min AS SELECT g, MIN(v) FROM t WHERE filter_col > 0 GROUP BY g;

function: wait_materialized_view_finish()

CREATE MATERIALIZED VIEW mv_max AS SELECT g, MAX(v) FROM t WHERE filter_col > 0 GROUP BY g;

function: wait_materialized_view_finish()

CREATE MATERIALIZED VIEW mv_distinct AS SELECT DISTINCT g FROM t WHERE filter_col > 0;

function: wait_materialized_view_finish()

CREATE MATERIALIZED VIEW mv_bitmap AS SELECT g, BITMAP_UNION(TO_BITMAP(v)) FROM t WHERE filter_col > 0 GROUP BY g;

function: wait_materialized_view_finish()

CREATE MATERIALIZED VIEW mv_hll AS SELECT g, HLL_UNION(HLL_HASH(v)) FROM t WHERE filter_col > 0 GROUP BY g;

function: wait_materialized_view_finish()

CREATE MATERIALIZED VIEW mv_bagg AS SELECT g, BITMAP_AGG(n) FROM t WHERE filter_col > 0 GROUP BY g;

function: wait_materialized_view_finish()

CREATE MATERIALIZED VIEW mv_avg AS SELECT g, AVG_UNION(AVG_STATE(v)) AS average_value FROM t WHERE filter_col > 0 GROUP BY g;

function: wait_materialized_view_finish()

ALTER TABLE t ADD ROLLUP plain_rollup(id, g, v) ORDER BY(id);

function: wait_alter_table_finish("ROLLUP", 8)

-- Qualifying INSERT and stream load after all sibling indexes activate exercise every maintenance expression.

INSERT INTO t VALUES (10,2,10,NULL,1);

shell: exec python3 '/tmp/range_sync_mv_test_range_sync_mv_${uuid0}/helper.py' 'print(load("9,3,9,9,1\n", "after"))__NL__assert q("SELECT COUNT(*) FROM t") == ((10,),)__NL__'

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT id, g, v FROM t ORDER BY id;

SELECT id, g, v FROM plain_rollup [_SYNC_MV_] ORDER BY id;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "plain_rollup";

function: check_hit_materialized_view('SELECT id, g, v FROM t ORDER BY id', 'plain_rollup')

SELECT id, g, v FROM t ORDER BY id;

SET query_including_mv_names = "";

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g, SUM(v), COUNT(n) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SELECT g, SUM(mv_sum_v), SUM(mv_count_n) FROM mv_sum [_SYNC_MV_] GROUP BY g ORDER BY g;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_sum";

function: check_hit_materialized_view('SELECT g, SUM(v), COUNT(n) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g', 'mv_sum')

SELECT g, SUM(v), COUNT(n) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SET query_including_mv_names = "";

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g, v FROM t WHERE filter_col > 0 ORDER BY g, v;

SELECT g, v FROM mv_projection [_SYNC_MV_] ORDER BY g, v;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_projection";

function: check_hit_materialized_view('SELECT g, v FROM t WHERE filter_col > 0 ORDER BY g, v', 'mv_projection')

SELECT g, v FROM t WHERE filter_col > 0 ORDER BY g, v;

SET query_including_mv_names = "";

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g + 10 AS shifted_g, v FROM t WHERE filter_col > 0 ORDER BY shifted_g, v;

SELECT mv_shifted_g, v FROM mv_computed [_SYNC_MV_] ORDER BY mv_shifted_g, v;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_computed";

function: check_hit_materialized_view('SELECT g + 10 AS shifted_g, v FROM t WHERE filter_col > 0 ORDER BY shifted_g, v', 'mv_computed')

SELECT g + 10 AS shifted_g, v FROM t WHERE filter_col > 0 ORDER BY shifted_g, v;

SET query_including_mv_names = "";

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g, MIN(v) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SELECT g, MIN(mv_min_v) FROM mv_min [_SYNC_MV_] GROUP BY g ORDER BY g;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_min";

function: check_hit_materialized_view('SELECT g, MIN(v) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g', 'mv_min')

SELECT g, MIN(v) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SET query_including_mv_names = "";

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g, MAX(v) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SELECT g, MAX(mv_max_v) FROM mv_max [_SYNC_MV_] GROUP BY g ORDER BY g;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_max";

function: check_hit_materialized_view('SELECT g, MAX(v) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g', 'mv_max')

SELECT g, MAX(v) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SET query_including_mv_names = "";

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT DISTINCT g FROM t WHERE filter_col > 0 ORDER BY g;

SELECT DISTINCT g FROM mv_distinct [_SYNC_MV_] ORDER BY g;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_distinct";

function: check_hit_materialized_view('SELECT DISTINCT g FROM t WHERE filter_col > 0 ORDER BY g', 'mv_distinct')

SELECT DISTINCT g FROM t WHERE filter_col > 0 ORDER BY g;

SET query_including_mv_names = "";

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g, BITMAP_UNION_COUNT(TO_BITMAP(v)) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SELECT g, BITMAP_UNION_COUNT(mv_bitmap_union_v) FROM mv_bitmap [_SYNC_MV_] GROUP BY g ORDER BY g;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_bitmap";

function: check_hit_materialized_view('SELECT g, BITMAP_UNION_COUNT(TO_BITMAP(v)) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g', 'mv_bitmap')

SELECT g, BITMAP_UNION_COUNT(TO_BITMAP(v)) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SET query_including_mv_names = "";

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g, COUNT(DISTINCT v) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SELECT g, BITMAP_UNION_COUNT(mv_bitmap_union_v) FROM mv_bitmap [_SYNC_MV_] GROUP BY g ORDER BY g;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_bitmap";

function: check_hit_materialized_view('SELECT g, COUNT(DISTINCT v) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g', 'mv_bitmap')

SELECT g, COUNT(DISTINCT v) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SET query_including_mv_names = "";

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g, HLL_UNION_AGG(HLL_HASH(v)) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SELECT g, HLL_UNION_AGG(mv_hll_union_v) FROM mv_hll [_SYNC_MV_] GROUP BY g ORDER BY g;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_hll";

function: check_hit_materialized_view('SELECT g, HLL_UNION_AGG(HLL_HASH(v)) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g', 'mv_hll')

SELECT g, HLL_UNION_AGG(HLL_HASH(v)) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SET query_including_mv_names = "";

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g, COUNT(DISTINCT n) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SELECT g, BITMAP_UNION_COUNT(mv_bitmap_union_n) FROM mv_bagg [_SYNC_MV_] GROUP BY g ORDER BY g;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_bagg";

function: check_hit_materialized_view('SELECT g, COUNT(DISTINCT n) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g', 'mv_bagg')

SELECT g, COUNT(DISTINCT n) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SET query_including_mv_names = "";

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g, CAST(ROUND(AVG(v), 6) AS DECIMAL(18,6)) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

-- Other binary columns and views borrowing a state type must not clear the MV's descriptor.

CREATE VIEW unrelated_binary AS SELECT HEX_DECODE_BINARY('FF') AS raw_value;

CREATE VIEW borrowed_state AS SELECT mv_average_value FROM mv_avg [_SYNC_MV_];

SELECT g, CAST(ROUND(AVG_MERGE(mv_average_value), 6) AS DECIMAL(18,6)) FROM mv_avg [_SYNC_MV_] GROUP BY g ORDER BY g;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_avg";

function: check_hit_materialized_view('SELECT g, CAST(ROUND(AVG(v), 6) AS DECIMAL(18,6)) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g', 'mv_avg')

SELECT g, CAST(ROUND(AVG(v), 6) AS DECIMAL(18,6)) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SET query_including_mv_names = "";

shell: exec python3 '/tmp/range_sync_mv_test_range_sync_mv_${uuid0}/helper.py' 'for mv in ("mv_sum", "mv_projection", "mv_computed", "mv_min", "mv_max", "mv_distinct", "mv_bitmap", "mv_hll", "mv_bagg", "mv_avg", "plain_rollup"):__NL__    assert latest(mv)["State"] == "FINISHED", latest(mv)__NL__for mv, fragment in (("mv_sum", "filter_col"), ("mv_computed", "shifted_g"), ("mv_avg", "average_value")):__NL__    ddl = str(q("SHOW CREATE MATERIALIZED VIEW " + mv)).lower()__NL__    assert fragment in ddl and "create materialized view" in ddl, ddl__NL__q("CREATE MATERIALIZED VIEW mv_cancel AS SELECT id, n FROM t")__NL__job = latest("mv_cancel")__NL__assert job["State"] not in ("FINISHED", "CANCELLED"), job__NL__q("CANCEL ALTER TABLE ROLLUP FROM t (" + str(job["JobId"]) + ")")__NL__deadline = time.monotonic() + 90__NL__while True:__NL__    done = latest("mv_cancel")__NL__    assert done["JobId"] == job["JobId"] and done["State"] != "FINISHED", done__NL__    if done["State"] == "CANCELLED":__NL__        break__NL__    assert time.monotonic() < deadline, done__NL__    time.sleep(0.1)__NL__cfg.wait_table_state_normal(db, "t", timeout_sec=10)__NL__q("INSERT INTO t VALUES (11,1,1,11,1)")__NL__q("SET enable_materialized_view_rewrite=false")__NL__q("SET enable_sync_materialized_view_rewrite=false")__NL__assert q("SELECT g, SUM(v), COUNT(n) FROM t WHERE filter_col>0 GROUP BY g ORDER BY g") == ((1,36,2),(2,47,1),(3,59,2))__NL__assert q("SELECT g, SUM(mv_sum_v), SUM(mv_count_n) FROM mv_sum [_SYNC_MV_] GROUP BY g ORDER BY g") == ((1,36,2),(2,47,1),(3,59,2))__NL__assert q("SELECT COUNT(*), SUM(v) FROM plain_rollup [_SYNC_MV_]") == ((11,262),)__NL__print("FINISHED siblings SHOW CREATE CANCELLED writes preserved")__NL__'

-- An empty source and a populated all-filtered source must both activate an empty index and maintain later qualifying data.

CREATE TABLE empty_t (id INT NOT NULL, g INT, v BIGINT, filter_col INT) DUPLICATE KEY(id) ORDER BY(id);

CREATE MATERIALIZED VIEW mv_empty AS SELECT g, SUM(v) FROM empty_t GROUP BY g;

function: wait_materialized_view_finish()

SELECT COUNT(*) FROM mv_empty [_SYNC_MV_];

INSERT INTO empty_t VALUES (1,1,9,-1);

CREATE MATERIALIZED VIEW mv_filtered AS SELECT g, SUM(v) FROM empty_t WHERE filter_col > 0 GROUP BY g;

function: wait_materialized_view_finish()

SELECT COUNT(*) FROM mv_filtered [_SYNC_MV_];

INSERT INTO empty_t VALUES (2,1,5,1);

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g, SUM(v) FROM empty_t GROUP BY g ORDER BY g;

SELECT g, SUM(mv_sum_v) FROM mv_empty [_SYNC_MV_] GROUP BY g ORDER BY g;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_empty";

function: check_hit_materialized_view('SELECT g, SUM(v) FROM empty_t GROUP BY g ORDER BY g', 'mv_empty')

SELECT g, SUM(v) FROM empty_t GROUP BY g ORDER BY g;

SET query_including_mv_names = "";

SET enable_materialized_view_rewrite = false;

SET enable_sync_materialized_view_rewrite = false;

SELECT g, SUM(v) FROM empty_t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SELECT g, SUM(mv_sum_v) FROM mv_filtered [_SYNC_MV_] GROUP BY g ORDER BY g;

SET enable_materialized_view_rewrite = true;

SET enable_sync_materialized_view_rewrite = true;

SET query_including_mv_names = "mv_filtered";

function: check_hit_materialized_view('SELECT g, SUM(v) FROM empty_t WHERE filter_col > 0 GROUP BY g ORDER BY g', 'mv_filtered')

SELECT g, SUM(v) FROM empty_t WHERE filter_col > 0 GROUP BY g ORDER BY g;

SET query_including_mv_names = "";

DROP MATERIALIZED VIEW mv_sum;

DROP MATERIALIZED VIEW mv_projection;

DROP MATERIALIZED VIEW mv_computed;

DROP MATERIALIZED VIEW mv_min;

DROP MATERIALIZED VIEW mv_max;

DROP MATERIALIZED VIEW mv_distinct;

DROP MATERIALIZED VIEW mv_bitmap;

DROP MATERIALIZED VIEW mv_hll;

DROP MATERIALIZED VIEW mv_bagg;

DROP MATERIALIZED VIEW mv_avg;

DROP MATERIALIZED VIEW mv_empty;

DROP MATERIALIZED VIEW mv_filtered;

ALTER TABLE t DROP ROLLUP plain_rollup;

INSERT INTO t VALUES (12,1,2,NULL,1);

SELECT g, SUM(v), COUNT(n) FROM t WHERE filter_col > 0 GROUP BY g ORDER BY g;

shell: exec python3 -c 'exec('"'"'import os,sys\nfrom pathlib import Path\np=Path(\'"'"'/tmp/range_sync_mv_test_range_sync_mv_${uuid0}/helper.py\'"'"')\nif p.exists():\n    os.execv(sys.executable, [sys.executable,str(p),"--cleanup"])\n'"'"')'

DROP DATABASE db_${uuid0} FORCE;
