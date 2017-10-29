package main;

import entity.Attribute;
import entity.Database;
import entity.SelectItem;
import entity.Table;

import java.util.List;

/**
 * @author I. Marcu
 */
public class Controller {

    public Controller() {

    }
//Berkley Functions ------------------------------------------------------------

    //INSERT entry into Table-------------------------------------------------------------
    public void insertEntry(List<String> listAttrValue, String dbName, String tbName) throws Exception {
    }

    // LOAD FROM TABLE--------------------------------------------------------------
// DELETE entry with specific Key-----------------------------------------------
    public void deleteEntry(String keyEntryValue, String dbName, String tbName) throws Exception {
    }

    public void createIndexForAttribute(String attrIndexStructure, String dbName, String tbName) throws Exception {
    }

    public void createDatabase(String dbName) throws Exception {
    }

    public void createTable(String dbName, String tbName) throws Exception {
    }

    public void deleteDbFromDatabases(String dbName) throws Exception {
    }

    public void addTableToDatabase(String dbName, Table tb) throws Exception {
    }

    public void deleteTableFromDatabase(String dbName, String tbName) throws Exception {

    }

    public void addAttributeToTable(String dbName, String tbName, String attrName, String attrType, boolean isPrimaryKey, boolean isUnique, boolean isNull, String refTable, String refAttr) throws Exception {
    }

    public void addIndexToTable(String attrName, String dbName, String tbName) throws Exception {
    }

    public void deleteAtributeFromTable(String dbName, String tbName, String attrName) throws Exception {
    }

    public List<Database> getDatabases() {
        return null;

    }

    public List<Table> getTablesOfDb(String dbName) {
        return null;

    }

    public List<Attribute> getAttributesOfTableOfDb(String dbName, String tbName) {
        return null;

    }

    public List<Attribute> getPrimayKeyAttributeOfTableOfDb(String dbName, String tbName) {
        return null;

    }

    public String toStringDatabase() {
        return null;
    }

    //LOAD for the moment only prints
    public String[][] loadDataFromTable(String dbName, String tbName) throws Exception {
        return null;

    }

    public List<String> loadDataFromTableInList(String dbName, String tbName) throws Exception {
        return null;

    }

    public void loadDataFromIndex(String attrIndexStructure, String dbName, String tbName) throws Exception {

    }

    public List<String> loadDataFromIndexInList(String attrIndexStructure, String dbName, String tbName) throws Exception {
        return null;

    }

    public List<String> select(List<SelectItem> forSelect) throws Exception {
        return null;

    }

    public List<String> lessThan(String column, String tbName, String dbName, String filter) throws Exception {
        return null;

    }

    public List<String> greaterThan(String column, String tbName, String dbName, String filter) throws Exception {
        return null;

    }

    public List<String> equalWith(String column, String tbName, String dbName, String filter) throws Exception {
        return null;

    }

    public List<String> lessThanOrEqualWith(String column, String tbName, String dbName, String filter) throws Exception {
        return null;

    }

    public List<String> greaterThanOrEqualWith(String column, String tbName, String dbName, String filter) throws Exception {
        return null;

    }

    public List<String> between(String column, String tbName, String dbName, String filter) throws Exception {
        return null;

    }

    public List<String> differentFrom(String column, String tbName, String dbName, String filter) throws Exception {
        return null;
    }


}
