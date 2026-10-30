CREATE VIEW `v_fact` AS
SELECT
  `test_prune`.`t_fact`.`id`,
  `test_prune`.`t_fact`.`part_key`,
  `test_prune`.`t_fact`.`name`,
  COALESCE(`test_prune`.`t_dim_b`.`b1`, `test_prune`.`t_fact`.`s1`) AS `s1`,
  COALESCE(`test_prune`.`t_dim_b`.`b3`, `test_prune`.`t_fact`.`d1`) AS `d1`,
  COALESCE(`test_prune`.`t_dim_b`.`b2`, `test_prune`.`t_fact`.`s2`) AS `s2`,
  COALESCE(`test_prune`.`t_dim_b`.`b4`, `test_prune`.`t_fact`.`t1`) AS `t1`,
  `test_prune`.`t_dim_a`.`a1`,
  `test_prune`.`t_dim_a`.`a2`,
  `test_prune`.`t_dim_c`.`c1`
FROM `test_prune`.`t_fact`
LEFT OUTER JOIN `test_prune`.`t_dim_a`
  ON  `test_prune`.`t_dim_a`.`id` = `test_prune`.`t_fact`.`id`
  AND `test_prune`.`t_dim_a`.`part_key` = `test_prune`.`t_fact`.`part_key`
LEFT OUTER JOIN `test_prune`.`t_dim_b`
  ON  `test_prune`.`t_fact`.`id` = `test_prune`.`t_dim_b`.`id`
  AND `test_prune`.`t_dim_b`.`part_key` = `test_prune`.`t_fact`.`part_key`
LEFT OUTER JOIN `test_prune`.`t_dim_c`
  ON  `test_prune`.`t_dim_c`.`id` = `test_prune`.`t_fact`.`id`
  AND `test_prune`.`t_dim_c`.`part_key` = `test_prune`.`t_fact`.`part_key`;
