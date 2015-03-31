create table links_raw(line STRING);

LOAD DATA INPATH '/data/in/links' OVERWRITE INTO TABLE links_raw;

CREATE TABLE links AS
SELECT split(line, '[",]')[1] AS lft, split(line, '[",]')[4] AS rght FROM links_raw;

SELECT ll2.lft AS lft, ll2.rght AS rght FROM
  (SELECT l1.lft AS lft, l1.rght AS rght
  FROM links l1 JOIN links l2
  ON l1.lft = l2.rght AND l1.rght = l2.lft) AS ll1
RIGHT JOIN links AS ll2
ON ll1.lft = ll2.lft AND ll1.rght = ll2.rght
WHERE ll1.lft IS NULL OR ll1.rght IS NULL;
