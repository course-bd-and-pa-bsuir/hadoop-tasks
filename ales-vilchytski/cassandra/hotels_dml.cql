USE hotels;

-- fullscan
SELECT * FROM hotels;
-- ////////

-- by address
SELECT * FROM hotels
WHERE address CONTAINS 'Moscow';
-- ////////

-- projection
SELECT name, rooms FROM hotels
WHERE name = 'Hotel 1';
-- ////////

-- simple aggregation
SELECT COUNT(*) FROM hotels;
-- ////////
