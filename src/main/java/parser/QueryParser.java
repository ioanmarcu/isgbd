package parser;

import entity.Database;
import main.Controller;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import util.DatabaseConnector;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author I. Marcu
 */
public class QueryParser {
    public static final String LINE_SEPARATOR = "\r\n";
    private List<Database> databases;
    private static final Logger LOG = Logger.getLogger(QueryParser.class.getName());
    private Database defaultDatabase;
    private Controller controller = new Controller();

    public QueryParser() {
        databases = DatabaseConnector.readDatabasesFromXML();
        defaultDatabase = databases != null ? databases.stream().findFirst().orElse(null) : null;
    }

    public String parse(String query) {
        refreshDatabases();
        String[] params = StringUtils.split(query, ", ");
        final String result;
        try {
            switch (params[0].toUpperCase()) {
                case SqlKeyword.CREATE: {
                    if (params[1].equalsIgnoreCase(SqlKeyword.TABLE)) {
                        result = createTable(Arrays.copyOfRange(params, 2, params.length));
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
                        databases = DatabaseConnector.readDatabasesFromXML();
                        if (CollectionUtils.isNotEmpty(databases)) {
                            result = StringUtils.join(databases.toArray(), LINE_SEPARATOR);
                        } else {
                            result = "No databases found";
                        }
                        break;
                    } else if (params[1].equalsIgnoreCase(SqlKeyword.DATABASES)) {
                        databases = DatabaseConnector.readDatabasesFromXML();
                        if (CollectionUtils.isNotEmpty(databases)) {
                            result = StringUtils.join(
                                    databases.stream().map(Database::getName).collect(Collectors.toList()),
                                    LINE_SEPARATOR);
                        } else {
                            result = "No databases found";
                        }
                        break;
                    }
                }
                case SqlKeyword.DROP: {
                    if (params[1].equalsIgnoreCase(SqlKeyword.TABLE)) {
                        result = dropTable(params[2]);
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
                case SqlKeyword.LIST: {
                    if (params[1].equalsIgnoreCase(SqlKeyword.TABLES)) {
                        result = listTables();
                    } else {
                        result = "Query not recognized. Please try again";
                    }
                    break;
                }
                default: {
                    result = "WARN: query could not be parsed";
                    break;
                }
            }
        } catch (Exception e) {
            LOG.severe(e.getMessage());
            LOG.severe(e.getCause().toString());
            return "ERROR while parsing the query. Please try again";
        }
        return result;
    }

    private String listTables() {
        return defaultDatabase != null ? StringUtils.join(defaultDatabase.getTables().toArray(), LINE_SEPARATOR) : "";
    }

    private String selectTable(String[] params) {
        return "";
    }

    private String dropTable(String tableName) throws Exception {
        controller.deleteTableFromDatabase(defaultDatabase.getName(), tableName);
        return "Successfully deleted table " + tableName;
    }

    private void refreshDatabases() {
        databases = DatabaseConnector.readDatabasesFromXML();
    }

    private String dropDatabase(String databaseName) throws Exception {
        controller.deleteDbFromDatabases(databaseName);
        return "Deleted database " + databaseName;
    }

    private String setDefaultDatabase(String databaseName) {
        final Database database =
                databases.stream().filter(d -> d.getName().equalsIgnoreCase(databaseName)).findFirst().orElse(null);

        if (database != null) {
            defaultDatabase = database;
            return "Database " + databaseName + " set as default";
        } else {
            return "Database not found. Default database is " + defaultDatabase.getName();
        }
    }

    private String createDatabase(String databaseName) throws Exception {
        controller.createDatabase(databaseName);
        return DatabaseConnector.createDatabase(databaseName);
    }

    private String createTable(String[] params) throws Exception {
        String tableName = params[0];
        String databaseName = defaultDatabase.getName();
        controller.createTable(databaseName, tableName);
        for (int i = 1; i < params.length - 1; i = i + 2) {
            controller.addAttributeToTable(databaseName, tableName, params[i], params[i + 1], false, false, false, null,
                    null);
        }
        return "Successfully created table " + tableName;

    }
}
