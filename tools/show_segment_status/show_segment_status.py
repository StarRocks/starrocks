#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import ConfigParser
import re
import sys
import os
import json
from urllib import urlopen

from fe_meta_resolver import FeMetaResolver 
from be_tablet_reslover import BeTabletResolver 

class Calc:
    def __init__(self, fe_meta, be_resolver):
        self.fe_meta = fe_meta
        self.be_resolver = be_resolver

    def calc_cluster_summary(self):
        self.calc_table_and_be_summary("", "", 0)
        return

    def calc_table_summary(self, db_name, table_name):
        self.calc_table_and_be_summary(db_name, table_name, 0)
        return

    def calc_be_summary(self, be_id):
        self.calc_table_and_be_summary("", "", be_id)
        return

    def calc_table_and_be_summary(self, db_candidates, tbl_candidates, be_id):
        total_rs_count = 0
        beta_rs_count = 0
        total_rs_size = 0
        beta_rs_size = 0
        total_rs_row_count = 0
        beta_rs_row_count = 0

        for tablet in self.fe_meta.get_all_tablets():
            # The db_name from meta contain cluster name, so use 'in' here
            if db_candidates and (not tablet['db_name'] in db_candidates):
                continue
            if tbl_candidates and (not tablet['tbl_name'] in tbl_candidates):
                continue;
            if be_id != 0 and tablet['be_id'] != be_id:
                continue
            rowsets = self.be_resolver.get_rowsets_by_tablet(tablet['tablet_id'])
            # If tablet has gone away, ignore it 
            if rowsets is None:
                continue
            for tablet_info in rowsets:
                total_rs_count += 1
                total_rs_row_count += tablet_info['num_rows']
                total_rs_size +=  tablet_info['data_disk_size']
                if tablet_info['is_beta']:
                    beta_rs_count += 1
                    beta_rs_size += tablet_info['data_disk_size']
                    beta_rs_row_count += tablet_info['num_rows']

        content_str = ""
        if db_candidates:
            content_str += ("db=%s " % db_candidates)
        if tbl_candidates:
            content_str += ("table=%s " % tbl_candidates)
        if be_id != 0:
            content_str += ("be=%s " % be_id)
        print "==========SUMMARY(%s)===========" % (content_str)
        print "rowset_count: %s / %s" % (beta_rs_count, total_rs_count)
        print "rowset_disk_size: %s / %s" % (beta_rs_size, total_rs_size)
        print "rowset_row_count: %s / %s" % (beta_rs_row_count, total_rs_row_count)
        print "==========================================================="
        return;

def main():
    cf = ConfigParser.ConfigParser()
    cf.read("./conf")
    fe_host = cf.get('cluster', 'fe_host')
    query_port = int(cf.get('cluster', 'query_port'))
    user = cf.get('cluster', 'user')
    query_pwd = cf.get('cluster', 'query_pwd')

    db_names = cf.get('cluster', 'db_names')
    table_names = cf.get('cluster', 'table_names')
    be_id = cf.getint('cluster', 'be_id')

    db_candidates = set()
    if db_names and db_names != '':
        db_name_list = db_names.split(',')
        for db_name in db_name_list:
            db_name = db_name.strip()
            if db_name != '':
                db_candidates.add(db_name)

    tbl_candidates = set()
    if table_names and table_names != '':
        table_name_list = table_names.split(',')
        for table_name in table_name_list:
            table_name = table_name.strip()
            if table_name != '':
                tbl_candidates.add(table_name)

    print "============= CONF ============="
    print "fe_host =", fe_host
    print "fe_query_port =", query_port
    print "user =", user 
    print "db_names =", db_candidates
    print "table_names =", tbl_candidates
    print "be_id =", be_id 
    print "===================================="

    fe_meta = FeMetaResolver(fe_host, query_port, user, query_pwd, db_candidates, tbl_candidates)
    fe_meta.init()
    fe_meta.debug_output()

    be_resolver = BeTabletResolver(fe_meta.be_list, fe_meta.tablet_map)
    be_resolver.init()
    be_resolver.debug_output()

    calc = Calc(fe_meta, be_resolver)
    calc.calc_cluster_summary()
    calc.calc_table_summary(db_candidates, tbl_candidates);
    calc.calc_be_summary(be_id);

if __name__ == '__main__':
    main()

