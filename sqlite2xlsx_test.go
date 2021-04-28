package main

import (
	"database/sql"
	"os"
	"reflect"
	"testing"

	"github.com/360EntSecGroup-Skylar/excelize/v2"
	_ "github.com/mattn/go-sqlite3"
)

func TestSqliteToXLSX(t *testing.T) {
	t.Cleanup(func() {
		removeIfExists := func(name string) {
			if _, err := os.Stat(name); err == nil || !os.IsNotExist(err) {
				if err = os.Remove(name); err != nil {
					t.Fatal(err)
				}
			}
		}
		removeIfExists("test.db")
		removeIfExists("test.xlsx")
	})

	db, err := sql.Open("sqlite3", "test.db")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
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
	`)
	if err != nil {
		t.Fatal(err)
	} else if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	res := map[string][][]string{
		"x": {
			{"a", "b", "c"},
			{"1", "first", "1.3"},
			{"2", "second", "2.5"},
			{"3", "third", "3.7"},
			{"4", "forth", "4.9"},
		},
		"y": {
			{"d", "e", "f"},
			{"6", "1.2", "1"},
			{"7", "2.4", "1"},
			{"8", "4.8", "2"},
			{"9", "9.6", "4"},
		},
		"z": {
			{"g", "h", "i"},
			{"6", "1.2", "p"},
			{"8", "4.8", "q"},
			{"9", "9.6", "r"},
		},
		"v": {
			{"a", "b", "c", "d"},
			{"1", "first", "1.3", "6"},
			{"1", "first", "1.3", "7"},
			{"2", "second", "2.5", "8"},
		},
		"y&x": {
			{"y.d", "y.e", "x.a", "x.b", "x.c"},
			{"6", "1.2", "1", "first", "1.3"},
			{"7", "2.4", "1", "first", "1.3"},
			{"8", "4.8", "2", "second", "2.5"},
			{"9", "9.6", "4", "forth", "4.9"},
		},
		"z&y&x": {
			{"z.i", "y.d", "y.e", "x.a", "x.b", "x.c"},
			{"p", "6", "1.2", "1", "first", "1.3"},
			{"q", "8", "4.8", "2", "second", "2.5"},
			{"r", "9", "9.6", "4", "forth", "4.9"},
		},
	}
	suit := []struct {
		pTables []string
		sheets  []string
	}{
		{[]string{"y", "z"}, []string{"y&x", "z&y&x", "x", "v"}},
		{[]string{}, []string{"x", "y", "z", "v"}},
	}

	for _, s := range suit {
		err = sqliteToXLSX("test.db", s.pTables...)
		if err != nil {
			t.Fatal(err)
		}
		f, err := excelize.OpenFile("test.xlsx")
		if err != nil {
			t.Fatal(err)
		}
		for _, sheet := range s.sheets {
			rows, err := f.GetRows(sheet)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(rows, res[sheet]) {
				t.Fatalf("sheet %s is wrong", sheet)
			}
		}
	}
}
