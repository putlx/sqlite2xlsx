import os
import sqlite3

if os.path.exists("test.db"):
	os.remove("test.db")

con = sqlite3.connect("test.db")
con.execute("PRAGMA foreign_keys = ON")
con.executescript("""
CREATE TABLE x (
	a INT PRIMARY KEY,
	b TEXT,
	c REAL
);
INSERT INTO x VALUES (1, 'first', 1.3);
INSERT INTO x VALUES (2, 'second', 2.5);
INSERT INTO x VALUES (3, 'third', 3.7);
INSERT INTO x VALUES (4, 'forth', 4.9);
CREATE TABLE y (
	d INT,
	e REAL,
	f INT REFERENCES x(a),
	PRIMARY KEY (d, e)
);
INSERT INTO y VALUES (6, 1.2, 1);
INSERT INTO y VALUES (7, 2.4, 1);
INSERT INTO y VALUES (8, 4.8, 2);
INSERT INTO y VALUES (9, 9.6, 4);
CREATE TABLE z (
	g INT,
	h REAL,
	i TEXT,
	FOREIGN KEY (g, h) REFERENCES y(d, e)
);
INSERT INTO z VALUES (6, 1.2, 'p');
INSERT INTO z VALUES (8, 4.8, 'q');
INSERT INTO z VALUES (9, 9.6, 'r');
CREATE VIEW v AS
SELECT a, b, c, d
FROM x INNER JOIN y
ON x.a == y.f
WHERE x.c < 4;
""")
con.commit()
