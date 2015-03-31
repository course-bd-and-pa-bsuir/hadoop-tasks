create table words(line STRING);

LOAD DATA INPATH '/data/in/words' OVERWRITE INTO TABLE words

SELECT word, count(1) AS count FROM
(SELECT explode(split(line, '[\s,"\']')) AS word FROM words) w
GROUP BY word
ORDER BY word;
