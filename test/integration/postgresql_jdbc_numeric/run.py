#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Opt-in real PostgreSQL / StarRocks numeric regression (requires pymysql and psql)."""
import argparse
from decimal import Decimal
import itertools
import json
import os
from pathlib import Path
import re
import subprocess
import uuid

import pymysql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--sr-host', default='127.0.0.1')
    parser.add_argument('--sr-port', type=int, default=9030)
    parser.add_argument('--sr-user', default='root')
    parser.add_argument('--pg-reader-role')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Output already exists')
    schema = 'sr_pg_numeric_' + uuid.uuid4().hex
    pg_command = json.loads(os.environ.get('PG_COMMAND', '["psql", "-X", "-v", "ON_ERROR_STOP=1"]'))
    report = {'schema': schema, 'cases': [], 'errors': []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    connection = None
    created = False

    def pg(sql):
        result = subprocess.run(pg_command, input=sql, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=60)
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout

    def sr(sql):
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def table(name):
        return '.'.join('`' + x.replace('`', '``') + '`' for x in (args.catalog, schema, name))

    def check(name, sql, expected=None, error=False, local=False):
        item = {'name': name, 'sql': sql, 'settings': dict(settings)}
        try:
            if error:
                try:
                    sr(sql)
                except pymysql.Error as failure:
                    item['error'] = str(failure)
                    assert 'cannot be represented exactly as DECIMAL(38,18)' in str(failure)
                else:
                    raise AssertionError('Expected strict numeric read error')
            else:
                actual = sr(sql)
                item['actual'] = actual
                if expected is not None:
                    assert actual == tuple(tuple(row) for row in expected), (actual, expected)
                plan = '\n'.join(str(row[0]) for row in sr('EXPLAIN VERBOSE ' + sql))
                item['plan'] = plan
                remote = re.findall(r'^\s*QUERY:\s*(.+)$', plan, re.M)
                item['remote_sql'] = remote
                if local:
                    assert remote, 'No remote SQL found'
                    assert all(not re.search(r'\bWHERE\b|\bGROUP\s+BY\b|\bJOIN\b|\bSUM\s*\(|\bAVG\s*\(', x, re.I)
                               for x in remote), remote
            item['passed'] = True
        except Exception as failure:
            item.update(passed=False, failure=str(failure))
        report['cases'].append(item)

    try:
        pg('CREATE SCHEMA ' + schema)
        created = True
        report['postgresql_version'] = pg('SELECT version()').strip()
        sql = (Path(__file__).parent / 'fixtures.sql').read_text().replace('{schema}', schema)
        invalid = ['100000000000000000000', '-100000000000000000000', '1e-19',
                   '1.1234567890123456789', 'NaN', 'Infinity', '-Infinity']
        for index, value in enumerate(invalid):
            sql += '\nCREATE TABLE {}.bad{} (id int, amount numeric);'.format(schema, index)
            sql += "INSERT INTO {}.bad{} VALUES (1, 1), (2, NULL), (3, '{}'::numeric);".format(schema, index, value)
        if args.pg_reader_role:
            role = '"' + args.pg_reader_role.replace('"', '""') + '"'
            sql += 'GRANT USAGE ON SCHEMA {} TO {};'.format(schema, role)
            sql += 'GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {};'.format(schema, role)
        pg(sql)
        connection = pymysql.connect(host=args.sr_host, port=args.sr_port, user=args.sr_user,
                                     password=os.environ.get('SR_PASSWORD', ''), autocommit=True,
                                     connect_timeout=10, read_timeout=90, charset='utf8mb4')
        sr('SET query_timeout=60')
        report['starrocks_version'] = sr('SELECT current_version()')
        report['description'] = sr('DESC ' + table('values_ok'))
        assert any(re.search(r'decimal(?:128)?\(38,18\)', str(row).replace(' ', ''), re.I)
                   for row in report['description']), report['description']
        expected_values = [(1, Decimal('1')), (2, Decimal('2')), (3, Decimal('10')),
                           (4, Decimal('-1.2')), (5, Decimal('1e-18')), (6, None),
                           (7, Decimal('1.123456789012345678')),
                           (8, Decimal('99999999999999999999.999999999999999999')),
                           (9, Decimal('-99999999999999999999.999999999999999999')), (10, Decimal('0'))]
        for agg, project, join in itertools.product((False, True), repeat=3):
            settings = dict(aggregate=agg, project=project, join=join, chunk_size=2 if not project else 512)
            sr('SET chunk_size=' + str(settings['chunk_size']))
            for key, enabled in [('agg', agg), ('project', project), ('join', join)]:
                sr('SET enable_jdbc_{}_push_down={}'.format(key, str(enabled).lower()))
            check('exact_read', 'SELECT id, amount FROM ' + table('values_ok') + ' ORDER BY id', expected_values)
            check('numeric_order', 'SELECT id FROM ' + table('small')
                  + ' WHERE amount IS NOT NULL ORDER BY amount,id', [(1,), (4,), (2,), (3,)], local=True)
            check('numeric_filter', 'SELECT id FROM ' + table('small') + ' WHERE amount > 2 ORDER BY id', [(3,)], local=True)
            check('projection', 'SELECT amount + 1 FROM ' + table('small') + ' WHERE id=1', [(Decimal('2'),)], local=True)
            check('group', 'SELECT amount,count(*) FROM ' + table('small')
                  + ' GROUP BY amount ORDER BY amount NULLS LAST',
                  [(Decimal('1'), 2), (Decimal('2'), 1), (Decimal('10'), 1), (None, 1)], local=True)
            check('aggregate', 'SELECT sum(amount),min(amount),max(amount) FROM ' + table('small'),
                  [(Decimal('14'), Decimal('1'), Decimal('10'))], local=True)
            check('join', 'SELECT a.id,b.id FROM ' + table('small') + ' a JOIN ' + table('small')
                  + ' b ON a.amount=b.amount WHERE a.id=2 ORDER BY a.id,b.id', [(2, 2)], local=True)
            check('view', 'SELECT amount FROM ' + table('numeric_view') + ' WHERE id=3', [(Decimal('10'),)], local=True)
            native = "table(`{}`.native_query('SELECT 1.25::numeric AS amount'))".format(args.catalog.replace("`", "``"))
            check('native_expression', 'SELECT amount FROM ' + native, [(Decimal('1.25'),)])
            native_bad = "table(`{}`.native_query('SELECT 1e-19::numeric AS amount'))".format(args.catalog.replace("`", "``"))
            check('native_reject', 'SELECT amount FROM ' + native_bad, error=True)
            check('aggregate_cannot_hide_overflow', 'SELECT sum(amount) FROM ' + table('cancelled'), error=True)
            check('project_cannot_hide_overflow', 'SELECT cast(amount AS DOUBLE) FROM ' + table('bad0'), error=True)
            check('many_chunks', 'SELECT id,amount FROM ' + table('many') + ' ORDER BY id',
                  [(i, None if i % 7 == 0 else Decimal(i) / 100) for i in range(1, 2060)])
            check('bounded_read', 'SELECT amount,exact FROM ' + table('bounded') + ' ORDER BY id',
                  [(Decimal('123456789012345678901.25'), Decimal('1.25')), (Decimal('.25'), Decimal('2.5'))])
            check('bounded_aggregate', 'SELECT sum(exact),sum(n),avg(n) FROM ' + table('bounded'),
                  [(Decimal('3.75'), 5, 2.5)])
            for index, value in enumerate(invalid):
                check('reject_' + value, 'SELECT amount FROM ' + table('bad' + str(index)), error=True)
                check('recover_' + str(index), 'SELECT amount FROM ' + table('small') + ' WHERE id=1', [(Decimal('1'),)])
        report['passed'] = all(x['passed'] for x in report['cases'])
    except Exception as failure:
        report['errors'].append(str(failure))
    finally:
        if connection is not None:
            connection.close()
        if created:
            try:
                pg('DROP SCHEMA ' + schema + ' CASCADE')
                report['cleaned_up'] = True
            except Exception as failure:
                report['errors'].append(str(failure))
        args.output.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps({'cases': len(report['cases']), 'failed': sum(not x['passed'] for x in report['cases']),
                      'errors': report['errors'], 'cleaned_up': report.get('cleaned_up', False)}))
    return 0 if report.get('passed') and not report['errors'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
