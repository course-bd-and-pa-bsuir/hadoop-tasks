-- database
DROP KEYSPACE IF EXISTS hotels;
CREATE KEYSPACE hotels
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE hotels;
-- //////

-- tables
DROP TABLE hotels;
CREATE TABLE hotels (
  id int PRIMARY KEY,
  name text,
  address map<text, text>,
  rooms frozen<list<map<text, text>>>
);
-- //////

-- indices
DROP INDEX name_idx;
CREATE INDEX name_idx ON hotels(name);

DROP INDEX address_values_idx;
CREATE INDEX address_values_idx ON hotels(address);
-- //////


-- seed data
INSERT INTO hotels (id, name, address, rooms)
VALUES (1, 'Hotel 1', { 'city': 'Minsk', 'street': 'Street 1, h1' }, [{ 'num': '1a', 'square': '10m' }, { 'num': '1b', 'square': '5m' }]);

INSERT INTO hotels (id, name, address, rooms)
VALUES (2, 'Hotel 2', { 'city': 'Moscow', 'street': 'Street 2, h2' }, [{ 'num': '2a', 'square': '20m' }]);

INSERT INTO hotels (id, name, address, rooms)
VALUES (3, 'Hotel 3', { 'city': 'Mogilev', 'street': 'Street 3, h3' }, [{ 'num': '3a', 'square': '30m' }]);

INSERT INTO hotels (id, name, address, rooms)
VALUES (4, 'Hotel 4', { 'city': 'Gomel', 'street': 'Street 4, h4' }, [{ 'num': '4a', 'square': '40m' }]);

INSERT INTO hotels (id, name, address, rooms)
VALUES (5, 'Hotel 5', { 'city': 'Smolensk', 'street': 'Street 5, h5' }, [{ 'num': '5a', 'square': '50m' }, { 'num': '5b', 'square': '55m' }, { 'num': '5c', 'square': '57m' }]);

INSERT INTO hotels (id, name, address, rooms)
VALUES (6, 'Hotel 6', { 'city': 'Kharkov', 'street': 'Street 6, h6' }, [{ 'num': '6a', 'square': '60m' }, { 'num': '6b', 'square': '6m' }]);

INSERT INTO hotels (id, name, address, rooms)
VALUES (7, 'Hotel 7', { 'city': 'Kiev', 'street': 'Street Str 7, h7' }, [{ 'num': '7a', 'square': '70m' }]);

INSERT INTO hotels (id, name, address, rooms)
VALUES (8, 'Hotel 8', { 'city': 'Riga', 'street': 'Street Str 8, h8' }, [{ 'num': '8a', 'square': '80m' }]);

INSERT INTO hotels (id, name, address, rooms)
VALUES (9, 'Hotel 9', { 'city': 'Warsaw', 'street': 'Street Str 9, h9' }, [{ 'num': '9a', 'square': '90m' }, { 'num': '9b', 'square': '9m' }]);

INSERT INTO hotels (id, name, address, rooms)
VALUES (10, 'Hotel 10', { 'city': 'Tallin', 'street': 'Street Str 10, h10' }, [{ 'num': '10a', 'square': '100m' }]);
-- ///////
