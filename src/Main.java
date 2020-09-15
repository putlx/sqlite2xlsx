import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;

public class Main {
    public static void main(String[] args) {
        for (var arg : args) {
            try {
                if (!(new File(arg)).isFile()) {
                    System.err.printf("no such file: \"%s\"\n", arg);
                    continue;
                }
                var output = (arg.toLowerCase().endsWith(".db") ? arg.substring(0, arg.length() - 3) : arg) + ".xls";
                if (sqlite2xls(arg, output)) {
                    System.out.printf("\"%s\" -> \"%s\"\n", arg, output);
                } else {
                    System.out.printf("empty database: \"%s\"\n", arg);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean sqlite2xls(String input, String output) throws IOException, SQLException {
        Workbook wb = new HSSFWorkbook();
        var sheetNum = 0;
        try (var con = DriverManager.getConnection("jdbc:sqlite:" + input)) {
            var dbmd = con.getMetaData();
            var tables = dbmd.getTables(null, null, null, null);
            for (; tables.next(); sheetNum++) {
                var tableName = tables.getString("TABLE_NAME");
                var sheet = wb.createSheet();
                wb.setSheetName(sheetNum, tableName);

                var rs = dbmd.getColumns(null, null, tableName, null);
                var columnTypes = new ArrayList<Integer>();
                var row = sheet.createRow(0);
                for (var columnNum = 0; rs.next(); columnNum++) {
                    row.createCell(columnNum).setCellValue(rs.getString("COLUMN_NAME"));
                    columnTypes.add(rs.getInt("DATA_TYPE"));
                }

                var statement = con.createStatement();
                statement.setQueryTimeout(3);
                rs = statement.executeQuery("SELECT * FROM " + tableName);
                for (var rowNum = 1; rs.next(); rowNum++) {
                    row = sheet.createRow(rowNum);
                    for (var colNum = 0; colNum < columnTypes.size(); colNum++) {
                        var cell = row.createCell(colNum);
                        switch (columnTypes.get(colNum)) {
                            case 4 -> cell.setCellValue(rs.getInt(colNum + 1));
                            case 6 -> cell.setCellValue(rs.getDouble(colNum + 1));
                            case 12 -> cell.setCellValue(rs.getString(colNum + 1));
                            default -> {
                                var data = rs.getBytes(colNum + 1);
                                cell.setCellValue(Base64.getEncoder().encodeToString(data));
                            }
                        }
                    }
                }
            }
        }
        if (sheetNum > 0) {
            try (var out = new FileOutputStream(output)) {
                wb.write(out);
            }
            return true;
        }
        return false;
    }
}
