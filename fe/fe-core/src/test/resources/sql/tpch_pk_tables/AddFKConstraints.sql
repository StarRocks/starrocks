ALTER TABLE partsupp SET("foreign_key_constraints" = "(ps_partkey) REFERENCES part(p_partkey);(ps_suppkey) REFERENCES supplier(s_suppkey)");
ALTER TABLE nation SET("foreign_key_constraints" = "(n_regionkey) REFERENCES region(r_regionkey)");
ALTER TABLE supplier SET("foreign_key_constraints" = "(s_nationkey) REFERENCES nation(n_nationkey)");
ALTER TABLE customer SET("foreign_key_constraints" = "(c_nationkey) REFERENCES nation(n_nationkey)");
ALTER TABLE orders SET("foreign_key_constraints" = "(o_custkey) REFERENCES customer(c_custkey)");
ALTER TABLE lineitem SET("foreign_key_constraints" = "(l_orderkey) REFERENCES orders(o_orderkey); (l_partkey) REFERENCES part(p_partkey);(l_suppkey) REFERENCES supplier(s_suppkey);(l_partkey,l_suppkey) REFERENCES partsupp(ps_partkey,ps_suppkey)");
