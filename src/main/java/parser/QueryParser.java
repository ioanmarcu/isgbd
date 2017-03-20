package parser;

import entity.Database;
import org.apache.commons.lang3.StringUtils;
import util.DatabaseConnector;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author I. Marcu
 */
public class QueryParser {
    private final DatabaseConnector databaseConnector;
    private List<Database> databases;
    private static final Logger LOG = Logger.getLogger(QueryParser.class.getName());
    private Database defaultDatabase;


    public QueryParser() {
        databaseConnector = new DatabaseConnector();
        databases = databaseConnector.readDatabasesFromXML();
    }

    public String parse(String query) {
        String[] params = StringUtils.split(query, ", ");
        final String result;
        try {
            switch (params[0].toUpperCase()) {
                case SqlKeyword.CREATE: {
                    if (params[1].equalsIgnoreCase(SqlKeyword.TABLE)) {
                        result = createTable(params);
                        break;
                    } else if (params[1].equalsIgnoreCase(SqlKeyword.DATABASE)) {
                        result = createDatabase(params[2]);
                        break;
                    }
                }
                case SqlKeyword.USE: {
                    result = setDefaultDatabase(params[1]);
                    break;
                }
                case SqlKeyword.SHOW: {
                    if (params[1].equalsIgnoreCase(SqlKeyword.ALL)) {
                        result = StringUtils.join(databaseConnector.readDatabasesFromXML().toArray());
                        break;
                    }
                }
                case SqlKeyword.DROP: {
                    if (params[1].equalsIgnoreCase(SqlKeyword.TABLE)) {
                        result = dropTable(params);
                        break;
                    } else if (params[1].equalsIgnoreCase(SqlKeyword.DATABASE)) {
                        result = dropDatabase(params[2]);
                        break;
                    }
                }
                case SqlKeyword.SELECT: {
                    result = selectTable(params);
                    break;
                }
                default: {
                    result = "WARN: query could not be parsed";
                    break;
                }
            }
        } catch (IndexOutOfBoundsException e) {
            LOG.log(Level.SEVERE, e.getMessage());
            return "ERROR while parsing the query. Please try again";
        }
        return result;
    }

    private String selectTable(String[] params) {
        return "";
    }

    private String dropTable(String[] params) {
        return "";
    }

    private String dropDatabase(String databaseName) {
        return databaseConnector.dropDatabase(databaseName);
    }

    private String setDefaultDatabase(String databaseName) {
        return "";
    }

    private String createDatabase(String databaseName) {
        return databaseConnector.createDatabase(databaseName);
    }

    private String createTable(String[] tableName) {
        return "";
    }
}
