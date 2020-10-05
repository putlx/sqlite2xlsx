import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringJoiner;

class Converter {
    private Workbook wb = null;
    private boolean empty = false;
    private HashMap<String, HashMap<String, TableDependencies.Column>> dependencies;

    public void read(String input, String[] pTableNames) throws SQLException {
        try (var con = DriverManager.getConnection("jdbc:sqlite:" + input)) {
            wb = new HSSFWorkbook();
            var sheetNum = 0;
            var dbmd = con.getMetaData();
            var included = new HashSet<String>();
            for (var tableName : pTableNames) {
                dependencies = new TableDependencies(dbmd).get(tableName);
                included.addAll(dependencies.keySet());
                var columnNames = new StringJoiner(", ");
                for (var table : dependencies.entrySet()) {
                    for (var column : table.getValue().values()) {
                        if (column.imported == null) {
                            columnNames.add(column.tableName + "." + column.name);
                        }
                    }
                }
                var pktables = new HashSet<String>();
                var conditions = new StringJoiner(" AND ");
                makeSQL(tableName, pktables, conditions);
                var sql = "SELECT " + columnNames + " FROM " + tableName;
                if (!pktables.isEmpty()) {
                    sql += " INNER JOIN " + String.join(", ", pktables) + " ON " + conditions;
                }
                var sheet = wb.createSheet();
                wb.setSheetName(sheetNum, String.join("&", dependencies.keySet()));
                sheetNum++;
                makeXLS(sheet, con, sql, true);
            }

            var tables = dbmd.getTables(null, null, null, null);
            while (tables.next()) {
                var tableName = tables.getString("TABLE_NAME");
                if (!included.contains(tableName)) {
                    var sheet = wb.createSheet();
                    wb.setSheetName(sheetNum, tableName);
                    sheetNum++;
                    makeXLS(sheet, con, "SELECT * FROM " + tableName, false);
                }
            }
            empty = sheetNum == 0;
        }
    }

    public void read(String input) throws SQLException {
        read(input, new String[]{});
    }

    public void write(String output) throws IOException {
        if (empty) {
            wb.createSheet();
        }
        try (var out = new FileOutputStream(output)) {
            wb.write(out);
        }
    }

    private void makeSQL(String tableName, HashSet<String> pktables, StringJoiner conditions) {
        for (var column : dependencies.get(tableName).values()) {
            if (column.imported != null) {
                pktables.add(column.imported.tableName);
                conditions.add(tableName + "." + column.name + " = " + column.imported.tableName + "." + column.imported.name);
                makeSQL(column.imported.tableName, pktables, conditions);
            }
        }
    }

    private void makeXLS(Sheet sheet, Connection con, String sql, boolean withTableName) throws SQLException {
        var statement = con.createStatement();
        statement.setQueryTimeout(3);
        var rs = statement.executeQuery(sql);
        var rsmd = rs.getMetaData();
        var colCount = rsmd.getColumnCount();
        var row = sheet.createRow(0);
        for (var colNum = 1; colNum <= colCount; colNum++) {
            var cellValue = rsmd.getColumnName(colNum);
            if (withTableName) {
                cellValue = rsmd.getTableName(colNum) + "." + cellValue;
            }
            row.createCell(colNum - 1).setCellValue(cellValue);
        }
        for (var rowNum = 1; rs.next(); rowNum++) {
            row = sheet.createRow(rowNum);
            for (var colNum = 1; colNum <= colCount; colNum++) {
                var cell = row.createCell(colNum - 1);
                switch (rsmd.getColumnType(colNum)) {
                    case 4 -> {
                        var value = rs.getInt(colNum);
                        if (!rs.wasNull()) {
                            cell.setCellValue(value);
                        }
                    }
                    case 7 -> {
                        var value = rs.getDouble(colNum);
                        if (!rs.wasNull()) {
                            cell.setCellValue(value);
                        }
                    }
                    case 12 -> {
                        var value = rs.getString(colNum);
                        if (!rs.wasNull()) {
                            cell.setCellValue(value);
                        }
                    }
                    default -> {
                        var data = rs.getBytes(colNum);
                        if (!rs.wasNull()) {
                            cell.setCellValue(Base64.getEncoder().encodeToString(data));
                        }
                    }
                }
            }
        }
    }
}
