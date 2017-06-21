/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package repository;

import entity.*;
import util.DatabaseConnector;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author I. Marcu
 */
public class RepoXML {

    List<Database> databases = DatabaseConnector.readDatabasesFromXML();
    RepoBerekeley repoB;

    public RepoXML(RepoBerekeley repoB) {
        this.repoB = repoB;
    }

    /*
    Create Database with dbName
    Add DB to databases
    Add DB to XML
     */
    public void createDatabase(String dbName) {
        Database db = new Database(dbName, new ArrayList<Table>());
        databases.add(db);
        DatabaseConnector.writeDatabasesToXML(databases);
    }

    /*
     Create Table tbName into DB with dbName
     Add Table to databases and XML structure
     */
    public void createTable(String dbName, String tbName) throws Exception {
        System.out.println("dbmsimpl.Controller.createTable()");
        Table tb = new Table(tbName, new ArrayList<Attribute>(), new ArrayList<IndexFile>());
        addTableToDatabase(dbName, tb);
        DatabaseConnector.writeDatabasesToXML(databases);
    }

    public void addTableToDatabase(String dbName, Table tb) {
        System.out.println("dbmsimpl.Controller.addTableToDatabase()");
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                db.addTableToDb(tb);
            }
        }
        DatabaseConnector.writeDatabasesToXML(databases);
    }

    public void deleteDbFromDatabases(String dbName) {
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                databases.remove(db);
            }
        }
        DatabaseConnector.writeDatabasesToXML(databases);
    }

    /*
    Delete Table tbName from DB dbName
     */
    public void deleteTableFromDatabase(String dbName, String tbName) {
        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName;
        File directory = new File(filename);
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (null != files) {
                for (int i = 0; i < files.length; i++) {
                    if (files[i].isDirectory()) {
                        DatabaseConnector.deleteDirectory(files[i]);
                    } else {
                        files[i].delete();
                    }
                }
            }
        }
        directory.delete();
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                db.deleteTableFromDb(tbName);
            }
        }
        DatabaseConnector.writeDatabasesToXML(databases);
    }

    /*
    Add Attribute(...) to Table tbName from DB dbName
     */
    public void addAttributeToTable(String dbName, String tbName, String attrName, String attrType, boolean isPrimaryKey, boolean isUnique, boolean isNull, String refTable, String refAttr) throws Exception {
        Attribute attr = new Attribute(attrName, attrType, isPrimaryKey, isUnique, isNull, refTable, refAttr);
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                for (Table tb : db.getTables()) {
                    if (tb.getName().equals(tbName)) {
                        tb.addAttrToTable(attr);
                        if (attr.isForeignKey() || attr.isUnique()) {
                            repoB.createIndexDefault(attrName, dbName, tbName);
                        }
                    }
                }
            }
        }
        DatabaseConnector.writeDatabasesToXML(databases);
    }

    /*
    Add Index to attr to Table tbName from DB dbName
    Add Index to XML
    Am mai modificat ceva la functia asta si nu mai merge bine trebuie sa o recopiez pe cea veche
     */
    public void addIndexToTable(String attrName, String dbName, String tbName) {
        boolean find = false;
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                for (Table tb : db.getTables()) {
                    if (tb.getName().equals(tbName)) {
                        for (IndexFile ix : tb.getIndexFiles()) {
                            if (ix.getIndexName().equals(attrName)) {
                                ix.addAttrNameToIndex(attrName);
                                find = true;
                            }
                        }
                        if (!find) {
                            List<String> indexAttr = new ArrayList<String>();
                            indexAttr.add(attrName);
                            tb.addIndexFile(new IndexFile(attrName));
                        }
                    }
                }
            }
        }
        DatabaseConnector.writeDatabasesToXML(databases);
    }

    /*
    Delete Attribute attrName from TAble tbName from DB dbName
     */
    public void deleteAtributeFromTable(String dbName, String tbName, String attrName) {
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                for (Table tb : db.getTables()) {
                    if (tb.getName().equals(tbName)) {
                        tb.deleteAttrToTable(attrName);
                    }
                }
            }
        }
        DatabaseConnector.writeDatabasesToXML(databases);
    }

    /*
    Get  list of all Databases with tables
     */
    public List<Database> getDatabases() {
        return databases;
    }

    /*
    Get list of all Tables from a DB dbName
     */
    public List<Table> getTablesOfDb(String dbName) {
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                return db.getTables();
            }
        }
        return null;
    }

    /*
    Get list of Attributes from a Table tbName from DB dbName
     */
    public List<Attribute> getAttributesOfTableOfDb(String dbName, String tbName) {
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                for (Table tb : db.getTables()) {
                    if (tb.getName().equals(tbName)) {
                        return tb.getAttributes();
                    }
                }
            }
        }
        return null;
    }

    /*
    Get list of Tables name which refer this table tbName
     */
    public List<String> getTablesNameRefOfTableOfDb(String dbName, String tbName) {
        List<String> listTb = new ArrayList<>();
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                for (Table tb : db.getTables()) {
                    for (Attribute a : tb.getAttributes()) {
                        if (a.isForeignKey() && a.getReferencedTable().equals(tbName)) {
                            listTb.add(tb.getName());
                        }
                    }
                }
            }
        }
        return listTb;
    }

    /*
    Get list pf PrimaryKey from a table
    In our case we use only one attribute like PrimaryKey
     */
    public List<Attribute> getPrimayKeyAttributeOfTableOfDb(String dbName, String tbName) {
        List<Attribute> listPK = new ArrayList<>();
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                for (Table tb : db.getTables()) {
                    if (tb.getName().equals(tbName)) {
                        for (Attribute a : tb.getAttributes()) {
                            if (a.isPrimaryKey()) {
                                listPK.add(a);
                            }
                        }
                    }
                }
            }

        }
        return listPK;
    }

    /*
    Verify if an attribute is unique
     */
    public boolean isUniqueAttribute(String attrName, String dbName, String tbName) {
        List<Attribute> attrList = getAttributesOfTableOfDb(dbName, tbName);
        for (Attribute a : attrList) {
            if (a.getAttrName().equals(attrName)) {
                return a.isUnique();
            }
        }
        return false;
    }

    /*
    Verify if an attribute is ForeignKey
     */
    public boolean isFkAttribute(String attrName, String dbName, String tbName) {
        List<Attribute> attrList = getAttributesOfTableOfDb(dbName, tbName);
        for (Attribute a : attrList) {
            if (a.getAttrName().equals(attrName)) {
                return a.isForeignKey();
            }
        }
        return false;
    }

    /*
    Get the index(possition) of the PK in the list of attributes from Table tbName DB dbName
    -1 if PK not exists
     */
    public int getIndexPKeyOfTableOfDb(String dbName, String tbName) {
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                for (Table tbFromDb : db.getTables()) {
                    if (tbFromDb.getName().equals(tbName)) {
                        List<Attribute> listAttr = tbFromDb.getAttributes();
                        for (int i = 0; i < listAttr.size(); i++) {
                            Attribute attr = listAttr.get(i);
                            if (attr.isPrimaryKey()) {
                                return i;
                            }
                        }
                    }
                }
            }
        }
        return -1;
    }

    public String getKeyValue(List<String> attrValues, String dbName, String tbName) {
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                for (Table tbFromDb : db.getTables()) {
                    if (tbFromDb.getName().equals(tbName)) {
                        List<Attribute> listAttr = tbFromDb.getAttributes();
                        for (int i = 0; i < listAttr.size(); i++) {
                            Attribute attr = listAttr.get(i);
                            if (attr.isPrimaryKey()) {
                                return attrValues.get(i);
                            }
                        }
                    }
                }
            }
        }
        return "";
    }

    public String getDataValue(List<String> attrValues, String dbName, String tbName) {
        String dataValue = "";
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                for (Table tbFromDb : db.getTables()) {
                    if (tbFromDb.getName().equals(tbName)) {
                        List<Attribute> listAttr = tbFromDb.getAttributes();
                        for (int i = 0; i < listAttr.size(); i++) {
                            Attribute attr = listAttr.get(i);
                            if (!attr.isPrimaryKey()) {
                                dataValue = dataValue + attrValues.get(i) + "#";
                            }
                        }
                    }
                }
            }
        }
        int endIndex = dataValue.length() - 1;
        dataValue = dataValue.substring(0, endIndex);
        return dataValue;
    }

    public List<Attribute> getDataStructure(String dbName, String tbName) {
        List<Attribute> dataStructure = new ArrayList<Attribute>();
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                for (Table tbFromDb : db.getTables()) {
                    if (tbFromDb.getName().equals(tbName)) {
                        List<Attribute> listAttr = tbFromDb.getAttributes();
                        for (int i = 0; i < listAttr.size(); i++) {
                            Attribute attr = listAttr.get(i);
                            if (!attr.isPrimaryKey()) {
                                dataStructure.add(attr);
                            }
                        }
                    }
                }
            }
        }
        return dataStructure;
    }

    public List<ForeignKey> getReferedTablesAndAttributeOfThisTable(String dbName, String tbName) {
        List<ForeignKey> refTables = new ArrayList<ForeignKey>();
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                for (Table tbFromDb : db.getTables()) {
                    List<Attribute> listAttr = tbFromDb.getAttributes();
                    for (int i = 0; i < listAttr.size(); i++) {
                        Attribute attr = listAttr.get(i);
                        if (attr.getReferencedTable().equals(tbName)) {
                            if (!attr.isPrimaryKey()) {
                                ForeignKey ref = new ForeignKey(tbFromDb.getName(), attr.getAttrName());
                                if (!refTables.contains(ref)) {
                                    refTables.add(ref);
                                }
                            }
                        }
                    }
                }
            }
        }
        return refTables;
    }

    public String getAttributeValueFromDataValue(String attrStructure, String dataValueConcat, String dbName, String tbName) {

        List<Attribute> listDataStructure = new ArrayList<Attribute>();
        String[] listStringValue = dataValueConcat.split("#");
        int indexAttr = 0;
        for (Database db : databases) {
            if (db.getName().equals(dbName)) {
                for (Table tbFromDb : db.getTables()) {
                    if (tbFromDb.getName().equals(tbName)) {
                        List<Attribute> listAttr = tbFromDb.getAttributes();
                        for (int i = 0; i < listAttr.size(); i++) {
                            Attribute attr = listAttr.get(i);
                            if (!attr.isPrimaryKey()) {
                                listDataStructure.add(attr);

                            }
                        }

                    }
                }
            }
        }
        for (Attribute a : listDataStructure) {
            if (a.getAttrName().equals(attrStructure)) {
                indexAttr = listDataStructure.indexOf(a);
            }
        }
        // System.out.println("INDEX ATTR = " + indexAttr + " table:" + tbName + " DB:" + dbName + " AttrStruct:" + attrStructure + " attrValue:" + listStringValue[indexAttr]);
        return listStringValue[indexAttr];
    }

    public String toStringDatabase() {
        String output = "";
        for (Database db : databases) {
            output = output + db.toString();
        }
        return output;
    }

    public Table getTableByName(String dbName, String tbName) {
        List<Table> tables = new ArrayList<Table>();
        tables = getTablesOfDb(dbName);
        for (Table tb : tables) {
            if (tb.getName().equals(tbName)) {
                return tb;
            }
        }
        return null;
    }

}
