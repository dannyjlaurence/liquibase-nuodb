package liquibase.database.ext;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.core.CreateSequenceStatement;

/**
 * NuoDB does not support all the standard sequence features.
 */
public class CreateSequenceGenerator extends AbstractSqlGenerator<CreateSequenceStatement> {
    @Override
    public int getPriority() {
        return SqlGenerator.PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateSequenceStatement statement, Database database) {
        return database instanceof NuoDBDatabase;
    }

    @Override
    public ValidationErrors validate(CreateSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("sequenceName", statement.getSequenceName());
//        validationErrors.checkDisallowedField("minValue", statement.getMinValue(), database, NuoDBDatabase.class);
//        validationErrors.checkDisallowedField("maxValue", statement.getMaxValue(), database, NuoDBDatabase.class);
//        validationErrors.checkDisallowedField("incrementBy", statement.getIncrementBy(), database, NuoDBDatabase.class);
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(CreateSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuffer buffer = new StringBuffer();
        buffer.append("CREATE SEQUENCE ");
        buffer.append(database.escapeSequenceName(statement.getSchemaName(), statement.getSequenceName()));
        if (statement.getStartValue() != null) {
            buffer.append(" START WITH ").append(statement.getStartValue());
        }
        return new Sql[]{new UnparsedSql(buffer.toString())};
    }
}
