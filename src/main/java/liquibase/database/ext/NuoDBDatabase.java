package liquibase.database.ext;

import liquibase.database.AbstractDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
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
        return false;
    }

    // IMPLEMENTATION DETAILS

    private static String[] getReservedWords() {
        return new String[]
                {
                        // Objects
                        "COLUMN", "CONSTRAINT", "DATABASE", "DOMAIN", "INDEX", "KEY", "ROLE",
                        "SCHEMA", "SEQUENCE", "TABLE", "TRIGGER", "USER", "VIEW",
                        // Types
                        "BIGINT", "BOOLEAN", "DATE", "DECIMAL", "DOUBLE PRECISION", "INTEGER",
                        "NUMERIC", "SMALLINT", "TIME", "TIMESTAMP", "VARCHAR",
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
