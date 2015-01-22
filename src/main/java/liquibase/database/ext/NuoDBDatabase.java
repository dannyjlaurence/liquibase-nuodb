package liquibase.database.ext;

import liquibase.database.AbstractDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Encapsulates NuoDB database support.
 *
 * @author Robert J. Buck
 */
public class NuoDBDatabase extends AbstractDatabase {

    public static final String PRODUCT_NAME = "NuoDB";

    private static final Set<String> RESERVED_WORDS;

    static {
        RESERVED_WORDS = new HashSet<String>(Arrays.asList(getReservedWords()));
    }

    public NuoDBDatabase() {
        super.setCurrentDateTimeFunction("NOW()");
    }

    @Override
    public String getLiquibaseSchemaName() {
        final String schemaName = getSchemaName(getConnection().getURL());
        return schemaName == null ? getDefaultSchemaName() : schemaName;
    }

    @Override
    public String getDefaultSchemaName() {
        final String defaultSchemaName = super.getDefaultSchemaName();
        final String schemaName = getSchemaName(getConnection().getURL());

        return defaultSchemaName == null ? schemaName : defaultSchemaName;
    }

    @Override
    public boolean isReservedWord(String word) {
        return RESERVED_WORDS.contains(word.toUpperCase());
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:com.nuodb:")) {
            return "com.nuodb.jdbc.Driver";
        }
        return null;
    }

    @Override
    protected String getDefaultDatabaseSchemaName() throws DatabaseException {
        trace("@getDefaultDatabaseSchemaName: catalog=" + getConnection().getCatalog());
        trace("@getDefaultDatabaseSchemaName: url=" + getConnection().getURL());
        return super.getDefaultDatabaseSchemaName();
    }

    @Override
    public String escapeTableName(String schemaName, String tableName) {
        if (schemaName == null) {
            schemaName = getLiquibaseSchemaName();
        }
        trace("@escapeTableName: " + schemaName + "@" + tableName);
        trace("@escapeTableName: defaultSchema=" + getDefaultSchemaName());
        return super.escapeTableName(schemaName, tableName);
    }

    @Override
    public String escapeViewName(String schemaName, String viewName) {
        return escapeDatabaseObject(viewName);
    }

    @Override
    public String escapeIndexName(String schemaName, String indexName) {
        return escapeDatabaseObject(indexName);
    }

    @Override
    public boolean isSystemTable(String catalogName, String schemaName, String tableName) {
        trace("@isSystemTable: " + schemaName + "@" + tableName);
        return super.isSystemTable(catalogName, schemaName, tableName) || schemaName.equals("SYSTEM");
    }

    @Override
    public String getTypeName() {
        return "nuodb";
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public String getCurrentDateTimeFunction() {
        return "NOW()";
    }

    @Override
    protected boolean generateAutoIncrementBy(BigInteger incrementBy) {
        return false;
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT;
    }

    @Override
    public boolean supportsSchemas() {
        return true;
    }

    // IMPLEMENTATION DETAILS

    private static final boolean ENABLE_TRACE = Boolean.parseBoolean(System.getProperty("liquibase.nuodb.trace", "false"));

    private static void trace(String message) {
        if (ENABLE_TRACE) {
            System.out.println(message);
        }
    }

    private static String getSchemaName(String url) {
        return extractProperties(url).getProperty("schema");
    }

    private static Properties extractProperties(String url) {
        Properties properties = new Properties();

        int p1 = url.indexOf("://") + 3;

        if (p1 > 0) {
            int p3 = url.indexOf('?', p1);
            if (p3 > 0) {
                String nvString = url.substring(p3 + 1);
                String[] nameValues = nvString.split("&");
                for (String nameValue : nameValues) {
                    String[] nv = nameValue.split("=");
                    if (nv.length == 2)
                        properties.put(nv[0], nv[1]);
                }

            }
        }

        return properties;
    }

    private static String[] getReservedWords() {
        return new String[]
                {
                        // Objects
                        "COLUMN", "CONSTRAINT", "DATABASE", "DOMAIN", "INDEX", "KEY", "ROLE",
                        "SCHEMA", "SEQUENCE", "TABLE", "TRIGGER", "USER", "VIEW",
                        // Types
                        "BIGINT", "BOOLEAN", "DATE", "DECIMAL", "DOUBLE", "FLOAT", "INTEGER",
                        "NUMERIC", "SMALLINT", "TIME", "TIMESTAMP", "VARCHAR",
                        // Type Modifiers
                        "PRECISION",
                        // Predicate Modifiers
                        "AND", "NOT",
                        // General Modifiers
                        "ALL", "AS", "BY", "DISTINCT", "ESCAPE", "FIRST", "FOR",
                        "GROUP", "HAVING", "INTO", "LIMIT", "NEXT", "ORDER", "PASSWORD",
                        "PRIMARY", "OFFSET", "ON", "ONLY", "ROW", "ROWS", "SET", "TO",
                        "UNION", "UNIQUE", "VALUES", "WHERE",
                        // Predicates
                        "BETWEEN", "CONTAINING", "LIKE",
                        // Math Functions
                        "ABS", "CEIL", "FLOOR", "POWER", "RADIANS", "ROUND", "SQRT",
                        // String Functions
                        "LOWER", "UPPER", "MOD",
                        // Special Values
                        "FALSE", "TRUE", "NULL",
                        // Ordering
                        "ASC", "DESC",
                        // Verbs
                        "ALTER", "CREATE", "DELETE", "DROP", "EXECUTE", "EXPLAIN", "INSERT",
                        "GRANT", "JOIN", "REPLACE", "REVOKE", "SELECT", "SHOW", "TRUNCATE",
                        "UPDATE", "USE",
                        // Join Types
                        "LEFT", "INNER", "FULL", "OUTER", "RIGHT",
                        // Transactions
                        "START", "COMMIT", "ONLY", "READ", "ROLLBACK", "TRANSACTION",
                        // Isolation Levels
                        "COMMITTED", "ISOLATION", "LEVEL", "READ", "SERIALIZABLE", "WRITE",
                };
    }
}
