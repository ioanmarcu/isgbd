/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package repository;

import com.sleepycat.je.*;
import entity.Attribute;
import entity.ForeignKey;

import java.io.File;
import java.util.List;

/**
 * @author I. Marcu
 */
public class ValidateBerkeley {

    RepoBerekeley repoB;
    RepoXML repoXML;

    public ValidateBerkeley(RepoBerekeley repoB, RepoXML repoXML) {
        this.repoB = repoB;
        this.repoXML = repoXML;
    }

    // true if entry is refered from another entry
    //false if entry is not refered from another entry and can be deleted
    public boolean verifyIfEntryIsReferedTables(String keyEntryValue, String dbName, String tbName) throws Exception {
        System.out.println("dbmsimpl.ValidateBerkeley.verifyIfEntryIsReferedTables()");
        boolean verify = false;

        List<ForeignKey> refTables = repoXML.getReferedTablesAndAttributeOfThisTable(dbName, tbName);

        for (ForeignKey ref : refTables) {
            if (verifyIfExistsKeyInIndexRefTable(keyEntryValue, ref.getReferencedAttribute(), dbName, ref.getReferencedTable())) {
                verify = true;
            }
        }
        return verify;
    }

    public boolean verifyIfExistsKeyInTable(String keyEntryValue, String dbName, String tbName) throws Exception {
        System.out.println("dbmsimpl.ValidateBerkeley.verifyIfExistsKeyInTable()");

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName;

        if (!(new File(filename).exists())) {
            throw new Exception("File path " + filename + " don't exist!");
        }

        Environment myDbEnvironment = null;
        com.sleepycat.je.Database myDatabase = null;
        Cursor myCursor = null;
        try {
            myDbEnvironment = new Environment(new File(filename), null);
            myDatabase = myDbEnvironment.openDatabase(null, tbName, null);
            myCursor = myDatabase.openCursor(null, null);

            DatabaseEntry theKey = new DatabaseEntry(keyEntryValue.getBytes("UTF-8"));
            DatabaseEntry theData = new DatabaseEntry();

            if (myDatabase.get(null, theKey, theData, LockMode.DEFAULT) != OperationStatus.SUCCESS) {
                System.out.println("ERROR verifyIfExistsKeyInTable: NO DATA WITH KEY = " + keyEntryValue + " IN Database:" + dbName + "IN Table:" + tbName + "  !!!");
                return false;
            } else {
                return true;
            }
        } catch (DatabaseException dbe) {
            // throw exception
            System.err.println("Error accessing database." + dbe);
        } finally {
            try {
                if (myCursor != null) {
                    myCursor.close();
                }
                if (myDatabase != null) {
                    myDatabase.close();
                }
                if (myDbEnvironment != null) {
                    myDbEnvironment.close();
                }
            } catch (DatabaseException dbe) {
                System.err.println("Error in close: " + dbe.toString());
            }
        }
        return false;
    }

    public boolean verifyIfExistsKeyInIndexRefTable(String myIndexKey, String attrIndexStructure, String dbName, String tbName) throws Exception {
        System.out.println("dbmsimpl.ValidateBerkeley.verifyIfExistsKeyInIndexRefTable()");

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName + "\\" + attrIndexStructure;

        if (!(new File(filename).exists())) {
            throw new Exception("File path " + filename + " don't exists!");
        }
        boolean verify = false;

        Environment myDbEnvironment = null;
        com.sleepycat.je.Database myDatabase = null;
        Cursor myCursor = null;
        try {
            // Open the environment, creating one if it does not exist
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            myDbEnvironment = new Environment(new File(filename), envConfig);

            // Open the database, creating one if it does not exist
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            myDatabase = myDbEnvironment.openDatabase(null, attrIndexStructure, dbConfig);
            myCursor = myDatabase.openCursor(null, null);

            DatabaseEntry theKey = new DatabaseEntry(myIndexKey.getBytes("UTF-8"));
            DatabaseEntry theData = new DatabaseEntry();

            if (myDatabase.get(null, theKey, theData, LockMode.DEFAULT) != OperationStatus.SUCCESS) {
                System.out.println("INFO verifyIfExistsKeyInIndexRefTable: NO DATA WITH KEY = " + myIndexKey + " in file " + filename + " !!!");
            } else {
                verify = true;
                throw new Exception("ERROR : The value is refered in Database:" + dbName + " Table:" + tbName + " search it in index AttrStruct:" + attrIndexStructure + " !!!");

            }
        } catch (DatabaseException de) {
            throw new Exception("Error DatabaseException verify index: " + de);
        } finally {
            try {
                if (myCursor != null) {
                    myCursor.close();
                }
            } catch (DatabaseException dbe) {
                throw new Exception("Error closing cursor index: " + dbe.toString());
            }
        }
        if (myDatabase
                != null) {
            myDatabase.close();
        }
        if (myDbEnvironment
                != null) {
            myDbEnvironment.close();
        }

        return verify;
    }

    public boolean verifyIfRefValuesExists(String dataStringConcatValue, String keyEntryValue, String dbName, String tbName) throws Exception {
        System.out.println("dbmsimpl.TestBerkley.verifyIfRefValuesExista()");

        List<Attribute> listAttr = repoXML.getDataStructure(dbName, tbName);

        String attrRefValue;

        for (Attribute attr : listAttr) {
            if (attr.isForeignKey()) {
                attrRefValue = repoXML.getAttributeValueFromDataValue(attr.getAttrName(), dataStringConcatValue, dbName, tbName);

                System.out.println("attrRefValue:" + attrRefValue);

                return verifyIfExistsKeyInTable(attrRefValue, dbName, attr.getReferencedTable());
            }
        }
        return true;
    }

    public boolean isIndexAttribute(String attrIndexStructure, String dbName, String tbName) {
        System.out.println("dbmsimpl.TestBerkley.isIndexAttribute()");

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName + "\\" + attrIndexStructure;

        return new File(filename).exists();
    }

}
