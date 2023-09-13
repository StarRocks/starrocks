ALTER TABLE A SET ("foreign_key_constraints" = "(a_pk) REFERENCES D(d_pk);(a_b_fk) REFERENCES B(b_pk)");
ALTER TABLE D SET ("foreign_key_constraints" = "(d_pk) REFERENCES A(a_pk);(d_e_fk) REFERENCES E(e_pk)");
ALTER TABLE B SET ("foreign_key_constraints" = "(b_ci_fk) REFERENCES CI(ci_pk);(b_cii_fk0, b_cii_fk1) REFERENCES CII(cii_pk0, cii_pk1)");
ALTER TABLE E SET ("foreign_key_constraints" = "(e_fi_fk) REFERENCES FI(fi_pk);(e_fii_fk0, e_fii_fk1) REFERENCES FII(fii_pk0, fii_pk1)");
