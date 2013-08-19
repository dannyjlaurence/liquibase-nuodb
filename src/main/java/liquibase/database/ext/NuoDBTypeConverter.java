package liquibase.database.ext;

import liquibase.database.Database;
import liquibase.database.structure.type.DataType;
import liquibase.database.structure.type.DateTimeType;
import liquibase.database.structure.type.NumberType;
import liquibase.database.structure.type.TinyIntType;
import liquibase.database.typeconversion.core.AbstractTypeConverter;

/**
 * Extends LiquiBase to support NuoDB type conversions.
 *
 * @author Robert J. Buck
 */
public class NuoDBTypeConverter extends AbstractTypeConverter {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof NuoDBDatabase;
    }

    @Override
    public TinyIntType getTinyIntType() {
        return new TinyIntType("SMALLINT");
    }

    @Override
    public NumberType getNumberType() {
        return new NumberType("NUMERIC");
    }

    @Override
    public DateTimeType getDateTimeType() {
        return new DateTimeType("TIMESTAMP");
    }

    @Override
    protected DataType getDataType(String columnTypeString, Boolean autoIncrement, String dataTypeName, String precision, String additionalInformation) {
        if (columnTypeString.equalsIgnoreCase("STRING")) {
            return new StringType();
        }
        return super.getDataType(columnTypeString, autoIncrement, dataTypeName, precision, additionalInformation);
    }
}
