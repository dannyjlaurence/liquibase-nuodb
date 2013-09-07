package liquibase.database.ext;

import liquibase.database.Database;
import liquibase.database.core.InformixDatabase;
import liquibase.database.core.OracleDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.database.structure.*;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.jvm.JdbcDatabaseSnapshotGenerator;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NuoDBDatabaseSnapshotGenerator extends JdbcDatabaseSnapshotGenerator {

    @Override
    public boolean supports(Database database) {
        return database instanceof NuoDBDatabase;
    }

    @Override
    public int getPriority(Database database) {
        return PRIORITY_DATABASE;
    }

    @Override
    protected void readIndexes(DatabaseSnapshot snapshot, String schema, DatabaseMetaData databaseMetaData) throws DatabaseException, SQLException {
        Database database = snapshot.getDatabase();
        updateListeners("Reading indexes for " + database.toString() + " ...");

        for (Table table : snapshot.getTables()) {
            ResultSet rs = null;
            Statement statement = null;
            try {
                if (database instanceof OracleDatabase) {
                    //oracle getIndexInfo is buggy and slow.  See Issue 1824548 and http://forums.oracle.com/forums/thread.jspa?messageID=578383&#578383
                    statement = ((JdbcConnection) database.getConnection()).getUnderlyingConnection().createStatement();
                    String sql = "SELECT INDEX_NAME, 3 AS TYPE, TABLE_NAME, COLUMN_NAME, COLUMN_POSITION AS ORDINAL_POSITION, null AS FILTER_CONDITION FROM ALL_IND_COLUMNS WHERE TABLE_OWNER='" + database.convertRequestedSchemaToSchema(schema) + "' AND TABLE_NAME='" + table.getName() + "' ORDER BY INDEX_NAME, ORDINAL_POSITION";
                    rs = statement.executeQuery(sql);
                } else {
                    rs = databaseMetaData.getIndexInfo(database.convertRequestedSchemaToCatalog(schema), database.convertRequestedSchemaToSchema(schema), table.getName(), false, true);
                }
                Map<String, Index> indexMap = new HashMap<String, Index>();
                while (rs.next()) {
                    String indexName = convertFromDatabaseName(rs.getString("INDEX_NAME"));
                    /*
                     * TODO Informix generates indexnames with a leading blank if no name given.
                     * An identifier with a leading blank is not allowed.
                     * So here is it replaced.
                     */
                    if (database instanceof InformixDatabase && indexName.startsWith(" ")) {
                        indexName = "_generated_index_" + indexName.substring(1);
                    }
                    short type = rs.getShort("TYPE");
                    //                String tableName = rs.getString("TABLE_NAME");
                    boolean nonUnique = true;
                    try {
                        nonUnique = rs.getBoolean("NON_UNIQUE");
                    } catch (SQLException e) {
                        //doesn't exist in all databases
                    }
                    String columnName = convertFromDatabaseName(rs.getString("COLUMN_NAME"));
                    short position = rs.getShort("ORDINAL_POSITION");
                    /*
                     * TODO maybe bug in jdbc driver? Need to investigate.
                     * If this "if" is commented out ArrayOutOfBoundsException is thrown
                     * because it tries to access an element -1 of a List (position-1)
                     */
                    if (database instanceof NuoDBDatabase
                            && type != DatabaseMetaData.tableIndexStatistic
                            && position == 0) {
                        System.out.println(this.getClass().getName() + ": corrected position to " + ++position);
                    }
                    String filterCondition = rs.getString("FILTER_CONDITION");

                    // hint, this is wrong in Nuo, we have a case where the index field is
                    // not a statistic, but the ordinal is set to zero!  <<<<<<<<<<<<<<< BUG IN NUO
                    if (type == DatabaseMetaData.tableIndexStatistic) {
                        continue;
                    }
                    //                if (type == DatabaseMetaData.tableIndexOther) {
                    //                    continue;
                    //                }

                    if (columnName == null) {
                        //nothing to index, not sure why these come through sometimes
                        continue;
                    }
                    Index indexInformation;
                    if (indexMap.containsKey(indexName)) {
                        indexInformation = indexMap.get(indexName);
                    } else {
                        indexInformation = new Index();
                        indexInformation.setTable(table);
                        indexInformation.setName(indexName);
                        indexInformation.setUnique(!nonUnique);
                        indexInformation.setFilterCondition(filterCondition);
                        indexMap.put(indexName, indexInformation);
                    }

                    for (int i = indexInformation.getColumns().size(); i < position; i++) {
                        indexInformation.getColumns().add(null);
                    }
                    indexInformation.getColumns().set(position - 1, columnName);
                }
                for (Map.Entry<String, Index> entry : indexMap.entrySet()) {
                    snapshot.getIndexes().add(entry.getValue());
                }
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException ignored) {
                    }
                }
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException ignored) {
                    }
                }
            }
        }

        Set<Index> indexesToRemove = new HashSet<Index>();

	    /*
        * marks indexes as "associated with" instead of "remove it"
	    * Index should have associations with:
	    * foreignKey, primaryKey or uniqueConstraint
	    * */
        for (Index index : snapshot.getIndexes()) {
            for (PrimaryKey pk : snapshot.getPrimaryKeys()) {
                if (index.getTable().getName().equalsIgnoreCase(pk.getTable().getName()) && index.getColumnNames().equals(pk.getColumnNames())) {
                    index.addAssociatedWith(Index.MARK_PRIMARY_KEY);
                }
            }
            for (ForeignKey fk : snapshot.getForeignKeys()) {
                if (index.getTable().getName().equalsIgnoreCase(fk.getForeignKeyTable().getName()) && index.getColumnNames().equals(fk.getForeignKeyColumns())) {
                    index.addAssociatedWith(Index.MARK_FOREIGN_KEY);
                }
            }
            for (UniqueConstraint uc : snapshot.getUniqueConstraints()) {
                if (index.getTable().getName().equalsIgnoreCase(uc.getTable().getName()) && index.getColumnNames().equals(uc.getColumnNames())) {
                    index.addAssociatedWith(Index.MARK_UNIQUE_CONSTRAINT);
                }
            }

        }
        snapshot.getIndexes().removeAll(indexesToRemove);
    }
}