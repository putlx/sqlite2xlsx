import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;

public class TableDependencies {
    private final DatabaseMetaData dbmd;
    private HashMap<String, HashMap<String, Column>> dependencies;

    public TableDependencies(DatabaseMetaData dbmd) {
        this.dbmd = dbmd;
    }

    public HashMap<String, HashMap<String, Column>> get(String tableName) throws SQLException {
        dependencies = new HashMap<>();
        set(tableName);
        return dependencies;
    }

    private void set(String tableName) throws SQLException {
        var columns = new HashMap<String, Column>();
        dependencies.put(tableName, columns);
        var pktableNames = new HashSet<String>();
        var rs = dbmd.getImportedKeys(null, null, tableName);
        while (rs.next()) {
            var pktableName = rs.getString("PKTABLE_NAME");
            var pkcolumnName = rs.getString("PKCOLUMN_NAME");
            var fkcolumnName = rs.getString("FKCOLUMN_NAME");
            pktableNames.add(pktableName);
            columns.put(fkcolumnName, new Column(fkcolumnName, tableName, new Column(pkcolumnName, pktableName)));
        }
        rs = dbmd.getColumns(null, null, tableName, null);
        while (rs.next()) {
            columns.computeIfAbsent(rs.getString("COLUMN_NAME"), k -> new Column(k, tableName));
        }
        for (var pktableName : pktableNames) {
            set(pktableName);
        }
    }

    static class Column {
        public final String name;
        public final String tableName;
        public Column imported;

        public Column(String name, String tableName, Column imported) {
            this.name = name;
            this.tableName = tableName;
            this.imported = imported;
        }

        public Column(String name, String tableName) {
            this(name, tableName, null);
        }
    }
}
