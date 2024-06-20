from pyhive import hive


def main():
    conn = hive.Connection(host="localhost", port=10000, username="root")

    with conn.cursor() as cur:
        cur.execute("show tables from hive_sink_bench_oss")
        rows = cur.fetchall()
        print(rows)

        cur.execute("SELECT * FROM hive_sink_bench_oss.lineitem_sf1 limit 10")
        rows = cur.fetchall()
        print(rows)


if __name__ == "__main__":
    main()
