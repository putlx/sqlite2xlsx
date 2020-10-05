import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Base64;
import java.util.HashSet;
import java.util.StringJoiner;

public class Converter {
    private DatabaseMetaData dbmd;
    private Workbook wb;
    private boolean empty;
    private HashSet<String> tableNames;
    private StringJoiner columnNames;
    private StringJoiner conditions;

    public void read(String input, String[] pTableNames) throws SQLException {
        try (var con = DriverManager.getConnection("jdbc:sqlite:" + input)) {
            dbmd = con.getMetaData();
            wb = new HSSFWorkbook();
            var sheetNum = 0;
            var included = new HashSet<String>();
            for (var tableName : pTableNames) {
                tableNames = new HashSet<>();
                columnNames = new StringJoiner(", ");
                conditions = new StringJoiner(" AND ");
                makeSQL(tableName);
                included.addAll(tableNames);
                tableNames.remove(tableName);
                var sql = "SELECT " + columnNames + " FROM " + tableName;
                if (!tableNames.isEmpty()) {
                    sql += " INNER JOIN " + String.join(", ", tableNames) + " ON " + conditions;
                }
                var sheet = wb.createSheet();
                tableNames.add(tableName);
                wb.setSheetName(sheetNum, String.join("&", tableNames));
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

    private void makeSQL(String tableName) throws SQLException {
        if (tableNames.contains(tableName)) {
            return;
        }
        tableNames.add(tableName);
        var fkcolumnNames = new HashSet<String>();
        var pktableNames = new HashSet<String>();
        var rs = dbmd.getImportedKeys(null, null, tableName);
        while (rs.next()) {
            var pktableName = rs.getString("PKTABLE_NAME");
            var pkcolumnName = rs.getString("PKCOLUMN_NAME");
            var fkcolumnName = rs.getString("FKCOLUMN_NAME");
            pktableNames.add(pktableName);
            fkcolumnNames.add(fkcolumnName);
            conditions.add(tableName + "." + fkcolumnName + " = " + pktableName + "." + pkcolumnName);
        }
        rs = dbmd.getColumns(null, null, tableName, null);
        while (rs.next()) {
            var columnName = rs.getString("COLUMN_NAME");
            if (!fkcolumnNames.contains(columnName)) {
                columnNames.add(tableName + "." + columnName);
            }
        }
        for (var pktableName : pktableNames) {
            makeSQL(pktableName);
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
