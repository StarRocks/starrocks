import trino

def main():
    conn = trino.dbapi.connect(
        host="localhost",
        port=9090,
        user="root",
    )

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM system.runtime.nodes")
        rows = cur.fetchall()
        print(rows)

        cur.execute("show tables from hive.hive_sink_bench_oss")
        rows = cur.fetchall()
        print(rows)


if __name__ == "__main__":
    main()
