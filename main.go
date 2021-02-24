package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/user"
	"reflect"
	"strings"

	"github.com/360EntSecGroup-Skylar/excelize/v2"
	_ "github.com/mattn/go-sqlite3"
)

func usage() {
	fmt.Println("usage: sqlite2xlsx database... [options]")
	fmt.Println("  -h, --help  display this information and exit")
	fmt.Println("  -p <TABLE>  primary tables, each of them recursively left joins its reference tables")
}

func main() {
	if len(os.Args) == 1 {
		usage()
		return
	}

	var databases, pTables []string
	for i, arg := range os.Args[1:] {
		if arg == "-p" {
			pTables = append(pTables, os.Args[i+2:]...)
			break
		} else if arg == "-h" || arg == "--help" {
			usage()
			return
		} else {
			databases = append(databases, arg)
		}
	}

	if len(pTables) > 0 && len(databases) > 1 {
		fmt.Println("more than one database included while primary tables specified")
		return
	}
	for _, database := range databases {
		if _, err := os.Stat(database); os.IsNotExist(err) {
			fmt.Println("no such file: " + database)
			continue
		}
		if err := sqliteToXLSX(database, pTables...); err != nil {
			fmt.Printf("%s: %s\n", database, err)
		}
	}
}

func sqliteToXLSX(dataSourceName string, pTables ...string) error {
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return err
	}
	defer db.Close()
	f := excelize.NewFile()
	sheetIndex := 0

	for _, pTable := range pTables {
		rows, err := db.Query("SELECT * FROM sqlite_master WHERE name = ? AND (type = 'table' OR type = 'view')", pTable)
		if err != nil {
			return err
		} else if !rows.Next() {
			rows.Close()
			return errors.New("no such table or view: " + pTable)
		} else if err = rows.Close(); err != nil {
			return err
		}

		tableSet := make(map[string]bool)
		var columns, conditions []string
		if err = findForeignKeys(db, pTable, &tableSet, &columns, &conditions); err != nil {
			return err
		}
		delete(tableSet, pTable)
		tables := make([]string, 0, len(tableSet))
		for table := range tableSet {
			tables = append(tables, table)
		}

		query := "SELECT " + strings.Join(columns, ", ") + " FROM " + pTable
		if len(tables) != 0 {
			query += " INNER JOIN " + strings.Join(tables, ", ") + " ON " + strings.Join(conditions, " AND ")
		}
		tables = append([]string{pTable}, tables...)
		sheetName := strings.Join(tables, "&")
		sheetIndex = f.NewSheet(sheetName)
		if err = makeXLSXFromSQL(f, sheetName, db, query, &columns); err != nil {
			return err
		}
	}

	tables, err := db.Query("SELECT name FROM sqlite_master WHERE type = 'table' OR type = 'view'")
	if err != nil {
		return err
	}
	for tables.Next() {
		var table string
		if err = tables.Scan(&table); err != nil {
			tables.Close()
			return err
		}
		stored := false
		for _, pTable := range pTables {
			if table == pTable {
				stored = true
				break
			}
		}
		if !stored {
			sheetIndex = f.NewSheet(table)
			if err = makeXLSXFromSQL(f, table, db, "SELECT * FROM "+table, nil); err != nil {
				tables.Close()
				return err
			}
		}
	}
	if err = tables.Close(); err != nil {
		return err
	}

	if sheetIndex != 0 {
		f.DeleteSheet("Sheet1")
	}
	if usr, err := user.Current(); err != nil {
		return err
	} else if err = f.SetDocProps(&excelize.DocProperties{Creator: usr.Username}); err != nil {
		return err
	}
	if strings.HasSuffix(dataSourceName, ".db") {
		dataSourceName = dataSourceName[:len(dataSourceName)-3]
	}
	dataSourceName += ".xlsx"
	if err = f.SaveAs(dataSourceName); err != nil {
		return err
	}
	return nil
}

func findForeignKeys(db *sql.DB, table string, tableSet *map[string]bool, columns, conditions *[]string) error {
	if (*tableSet)[table] {
		return nil
	}
	(*tableSet)[table] = true

	fkColumns := make(map[string]bool)
	pkTables := make(map[string]bool)
	rows, err := db.Query("SELECT * FROM pragma_foreign_key_list(?)", table)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, seq int64
		var pkTable, from, to, onUpdate, onDelete, match string
		if err = rows.Scan(&id, &seq, &pkTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			rows.Close()
			return err
		}
		pkTables[pkTable] = true
		fkColumns[from] = true
		*conditions = append(*conditions, table+"."+from+" = "+pkTable+"."+to)
	}
	if err = rows.Close(); err != nil {
		return err
	}

	rows, err = db.Query("SELECT * FROM pragma_table_info(?)", table)
	if err != nil {
		return err
	}
	for rows.Next() {
		var cid, notnull, pk int64
		var name, tp string
		var dfltValue interface{}
		if err = rows.Scan(&cid, &name, &tp, &notnull, &dfltValue, &pk); err != nil {
			rows.Close()
			return err
		}
		if !fkColumns[name] {
			*columns = append(*columns, table+"."+name)
		}
	}
	if err = rows.Close(); err != nil {
		return err
	}

	for pkTable := range pkTables {
		if err = findForeignKeys(db, pkTable, tableSet, columns, conditions); err != nil {
			return err
		}
	}
	return nil
}

func makeXLSXFromSQL(f *excelize.File, sheetName string, db *sql.DB, query string, columns *[]string) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	first := rows.Next()
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	var cells []interface{}
	for i, columnType := range columnTypes {
		cells = append(cells, reflect.New(columnType.ScanType()).Interface())
		cellName, err := excelize.CoordinatesToCellName(i+1, 1)
		if err != nil {
			return err
		}
		var column string
		if columns != nil {
			column = (*columns)[i]
		} else {
			column = columnType.Name()
		}
		if err = f.SetCellStr(sheetName, cellName, column); err != nil {
			return err
		}
	}

	for rowIndex := 2; first || rows.Next(); rowIndex++ {
		first = false
		if err = rows.Scan(cells...); err != nil {
			return err
		}
		for i := range columnTypes {
			cellName, err := excelize.CoordinatesToCellName(i+1, rowIndex)
			if err != nil {
				return err
			}
			if err = f.SetCellValue(sheetName, cellName, reflect.ValueOf(cells[i]).Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}
