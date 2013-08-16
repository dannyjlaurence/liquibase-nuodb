package liquibase.database.ext;

import liquibase.database.structure.type.TextType;

/**
 * Support custom NuoDB string type in LiquiBase.
 *
 * @author Robert J. Buck
 */
public class StringType extends TextType {

    public StringType() {
        super("STRING", 0, 0);
    }

    @Override
    public boolean getSupportsPrecision() {
        return false;
    }
}
