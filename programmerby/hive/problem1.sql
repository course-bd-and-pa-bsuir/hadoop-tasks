-- Problem 1

CREATE TABLE links (line STRING);

LOAD DATA INPATH '/user/guest/links.json' OVERWRITE INTO TABLE links;

CREATE TABLE links2 AS
    SELECT get_json_object(concat('{"z":', links.line, '}'), "$.z[0]") AS n1,
           get_json_object(concat('{"z":', links.line, '}'), "$.z[1]") AS n2 FROM links;

SELECT z1.n1, z1.n2
FROM links2 z1
LEFT JOIN links2 z2 ON (z1.n1 = z2.n2 AND z1.n2 = z2.n1)
WHERE z2.n1 IS NULL;

