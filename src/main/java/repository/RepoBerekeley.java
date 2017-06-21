/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package repository;

import com.sleepycat.je.*;
import entity.Attribute;
import entity.SelectItem;

import java.io.File;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

class Struct {
    public String attr;
    public int sum;
}

class Struct2 {
    public String attr;
    public int nr;
}

class Struct3 {
    public String attr;
    public int sum;
    public int nr;
    public int avg;
}

/**
 * @author I. Marcu
 */
public class RepoBerekeley {

    RepoXML repoXML = new RepoXML(this);
    ValidateBerkeley validate = new ValidateBerkeley(this, repoXML);

    public RepoBerekeley() {
    }

    public void createIndexDefault(String attrIndexStructure, String dbName, String tbName) throws Exception {
        System.out.println("dbmsimpl.TestBerkley.createIndexDefault()");
        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName + "\\" + attrIndexStructure;

        if (!(new File(filename).exists())) {
            new File(filename).mkdirs();
        }

        if (!(new File(filename).exists())) {
            throw new Exception("File path " + filename + " can't be created!");
        }

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

        } catch (DatabaseException de) {
            throw new Exception("Error DatabaseException insert index: " + de);
        } finally {
            try {
                if (myCursor != null) {
                    myCursor.close();
                }
            } catch (DatabaseException dbe) {
                throw new Exception("Error closing cursor index: " + dbe.toString());
            }
        }
        if (myDatabase != null) {
            myDatabase.close();
        }
        if (myDbEnvironment != null) {
            myDbEnvironment.close();
        }
    }

    public void createIndexForAttribute(String attrIndexStructure, String dbName, String tbName) throws Exception {

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName;

        if (!(new File(filename).exists())) {
            new File(filename).mkdirs();
        }

        if (!(new File(filename).exists())) {
            throw new Exception("File path " + filename + " can't be created!");
        }

        Environment myDbEnvironment = null;
        com.sleepycat.je.Database myDatabase = null;
        Cursor myCursor = null;
        try {
            myDbEnvironment = new Environment(new File(filename), null);
            myDatabase = myDbEnvironment.openDatabase(null, tbName, null);
            myCursor = myDatabase.openCursor(null, null);

            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            while (myCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                String keyString = new String(foundKey.getData(), "UTF-8");
                String dataString = new String(foundData.getData(), "UTF-8");

                System.out.println("Key | Data : " + keyString + " | "
                        + dataString + "");

                String myIndexKey = repoXML.getAttributeValueFromDataValue(attrIndexStructure, dataString, dbName, tbName);
                String myIndexData = keyString;

                System.out.println("myIndexKey:" + myIndexKey);
                System.out.println("myIndexData:" + myIndexData);

                if (repoXML.isUniqueAttribute(attrIndexStructure, dbName, tbName)) {
                    insertIndexUniqueEntry(attrIndexStructure, myIndexKey, myIndexData, dbName, tbName);
                } else {
                    insertIndexNonUniqueEntry(attrIndexStructure, myIndexKey, myIndexData, dbName, tbName);
                }
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

    }

    public void createIndexForNewEntryIfExists(String dataStringConcatValue, String entryKey, String dbName, String tbName) throws Exception {
        System.out.println("dbmsimpl.TestBerkley.createIndexForNewEntryIfExists()");

        List<Attribute> listAttr = repoXML.getDataStructure(dbName, tbName);

        String myIndexKey = "";
        String myIndexData = entryKey;

        for (Attribute attr : listAttr) {
            if (validate.isIndexAttribute(attr.getAttrName(), dbName, tbName)) {
                myIndexKey = repoXML.getAttributeValueFromDataValue(attr.getAttrName(), dataStringConcatValue, dbName, tbName);

                System.out.println("myIndexKey:" + myIndexKey);
                System.out.println("myIndexData:" + myIndexData);

                if (repoXML.isUniqueAttribute(attr.getAttrName(), dbName, tbName)) {
                    insertIndexUniqueEntry(attr.getAttrName(), myIndexKey, myIndexData, dbName, tbName);
                } else {
                    insertIndexNonUniqueEntry(attr.getAttrName(), myIndexKey, myIndexData, dbName, tbName);
                }
            }
        }

    }

    public void deleteEntry(String keyEntryValue, String dbName, String tbName) throws Exception {
        System.out.println("dbmsimpl.TestBerkley.deleteEntry()");

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName;

        if (!(new File(filename).exists())) {
            new File(filename).mkdirs();
        }

        if (!(new File(filename).exists())) {
            throw new Exception("File path " + filename + " can't be created!");
        }

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
            myDatabase = myDbEnvironment.openDatabase(null, tbName, dbConfig);
            myCursor = myDatabase.openCursor(null, null);

            DatabaseEntry theKey = new DatabaseEntry(keyEntryValue.getBytes("UTF-8"));
            DatabaseEntry theData = new DatabaseEntry();

            if (myDatabase.get(null, theKey, theData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                if (!validate.verifyIfEntryIsReferedTables(keyEntryValue, dbName, tbName)) {
                    deleteIndexEntryIfExists(keyEntryValue, dbName, tbName);
                    if (myDatabase.delete(null, theKey) != OperationStatus.SUCCESS) {
                        throw new Exception("ERROR deleteEntry: KEY = " + keyEntryValue + "key exists, data can't be deleted !!!");
                    } else {
                        System.out.println("ENTRY with KEY:" + keyEntryValue + " was deleted from index file:" + filename);
                    }
                }
            } else {
                throw new Exception("ERROR deleteEntry: KEY = " + keyEntryValue + " DON'T EXISTS IN THE TABLE " + tbName + ", data can't be deleted !!!");
            }
        } catch (DatabaseException de) {
            throw new Exception(" ERROR delete from table: " + de);
        } finally {
            try {
                if (myCursor != null) {
                    myCursor.close();
                }
            } catch (DatabaseException dbe) {
                throw new Exception(" ERROR closing cursor: " + dbe.toString());
            }
        }
        if (myDatabase != null) {
            myDatabase.close();
        }
        if (myDbEnvironment != null) {
            myDbEnvironment.close();
        }

    }

    public void deleteIndexEntry(String attrIndexStructure, String myIndexKey, String myIndexData, String dbName, String tbName) throws Exception {
        System.out.println("dbmsimpl.TestBerkley.deleteIndexEntry()");

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName + "\\" + attrIndexStructure;

        if (!(new File(filename).exists())) {
            throw new Exception("File path " + filename + " can't be created!");
        }

        boolean deleteFile = false;

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
                System.out.println("ERROR deleteIndexEntry: NO DATA WITH KEY = " + myIndexKey + " in file " + filename + " !!!");
                theData = new DatabaseEntry(myIndexData.getBytes("UTF-8"));

                System.out.println("----------------------------------------------->key: " + myIndexKey);
                System.out.println("----------------------------------------------->values: " + myIndexData);

            } else {

                String dataStringValue = new String(theData.getData(), "UTF-8");

                if (dataStringValue.contains("#")) {

                    if (dataStringValue.indexOf(myIndexData + "#") == 0) {
                        dataStringValue = dataStringValue.replaceAll(myIndexData + "#", "");
                    } else {
                        dataStringValue = dataStringValue.replaceAll("#" + myIndexData, "");
                    }
                } else {
                    dataStringValue = "";
                }

                System.out.println("----------------------------------------------->key: " + myIndexKey);
                System.out.println("----------------------------------------------->values: " + dataStringValue);

                theData = new DatabaseEntry(dataStringValue.getBytes("UTF-8"));

                if (!dataStringValue.isEmpty()) {
                    if (myDatabase.put(null, theKey, theData) != OperationStatus.SUCCESS) {
                        throw new Exception("ERROR : INSERT index non unique!!! KEY:" + myIndexKey + " fi le:" + filename);
                    }
                } else if (myDatabase.delete(null, theKey) != OperationStatus.SUCCESS) {
                    System.out.println("KEY from index have to be deleted but it is not possible, file:" + filename + "!");
                } else {
                    System.out.println("KEY deleted from index" + myIndexKey + " fi le:" + filename);
                }
            }

            if (myDatabase.count() == 0) {
                deleteFile = true;
            }
        } catch (DatabaseException de) {
            throw new Exception("Error DatabaseException insert index: " + de);
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

        if (deleteFile) {
            System.out.println("File " + filename + "of index is deleted because has no data");
            deleteIndexFile(attrIndexStructure, dbName, tbName);

        }
    }

    public void deleteIndexEntryIfExists(String keyEntryValue, String dbName, String tbName) throws Exception {

        List<Attribute> listAttr = repoXML.getDataStructure(dbName, tbName);

        String myIndexKey = "";
        String myIndexData = keyEntryValue;

        for (Attribute attr : listAttr) {
            if (validate.isIndexAttribute(attr.getAttrName(), dbName, tbName)) {
                String dataEntryValue = getDataByKeyFromTable(keyEntryValue, dbName, tbName);
                myIndexKey = repoXML.getAttributeValueFromDataValue(attr.getAttrName(), dataEntryValue, dbName, tbName);

                System.out.println("myIndexKey:" + myIndexKey);
                System.out.println("myIndexData:" + myIndexData);

                deleteIndexEntry(attr.getAttrName(), myIndexKey, myIndexData, dbName, tbName);
            }
        }

    }

    public boolean deleteIndexFile(String attrIndexStructure, String dbName, String tbName) {
        System.out.println("dbmsimpl.TestBerkley.deleteIndexFile()");
        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName + "\\" + attrIndexStructure;

        return new File(filename).delete();
    }

    public String getDataByKeyFromTable(String keyEntryValue, String dbName, String tbName) throws Exception {

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
                System.out.println("ERROR getDataByKeyFromTable : NO DATA WITH KEY = " + keyEntryValue + " Database:" + dbName + " Table:" + tbName + "!!!");
                return "";
            } else {
                return new String(theData.getData(), "UTF-8");
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
        return "";
    }

    public void insertEntry(List<String> listAttrValue, String dbName, String tbName) throws Exception {
        System.out.println("dbmsimpl.TestBerkley.insertEntry()");

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName;

        if (!(new File(filename).exists())) {
            new File(filename).mkdirs();
        }

        if (!(new File(filename).exists())) {
            throw new Exception("File path " + filename + " can't be created!");
        }
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
            myDatabase = myDbEnvironment.openDatabase(null, tbName, dbConfig);
            myCursor = myDatabase.openCursor(null, null);

            String keyEntryValue = repoXML.getKeyValue(listAttrValue, dbName, tbName);
            String dataEntryValue = repoXML.getDataValue(listAttrValue, dbName, tbName);

            System.out.println("----------------------------------------------->key: " + keyEntryValue);
            System.out.println("----------------------------------------------->values: " + dataEntryValue);

            if (validate.verifyIfRefValuesExists(dataEntryValue, keyEntryValue, dbName, tbName)) {

                DatabaseEntry theKey = new DatabaseEntry(keyEntryValue.getBytes("UTF-8"));
                DatabaseEntry theData = new DatabaseEntry(dataEntryValue.getBytes("UTF-8"));
                if (myDatabase.putNoOverwrite(null, theKey, theData) != OperationStatus.SUCCESS) {
                    throw new Exception("ERROR insertEntry: DUPLICATE KEY, data can't be overwrite!!!");
                } else {
                    System.out.println("GO FOR INDEX--------------------------------");
                    createIndexForNewEntryIfExists(dataEntryValue, keyEntryValue, dbName, tbName);

                }
            } else {
                throw new Exception("The refered value not exist!");
            }
        } catch (DatabaseException de) {
            throw new Exception("Error DatabaseException insert table: " + de);
        } finally {
            try {
                if (myCursor != null) {
                    myCursor.close();
                }
            } catch (DatabaseException dbe) {
                throw new Exception("Error closing cursor: " + dbe.toString());
            }
        }
        if (myDatabase != null) {
            myDatabase.close();
        }
        if (myDbEnvironment != null) {
            myDbEnvironment.close();
        }
    }

    public void insertIndexNonUniqueEntry(String attrIndexStructure, String myIndexKey, String myIndexData, String dbName, String tbName) throws Exception {
        System.out.println("dbmsimpl.TestBerkley.insertIndexUniqueEntry()");

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName + "\\" + attrIndexStructure;

        if (!(new File(filename).exists())) {
            new File(filename).mkdirs();
        }

        if (!(new File(filename).exists())) {
            throw new Exception("File path " + filename + " can't be created!");
        }

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
                System.out.println("INFO insertIndexNonUniqueEntry : NO DATA WITH KEY = " + myIndexKey + " SO INSERT NEW ENTRY TO INDEX !!!");
                theData = new DatabaseEntry(myIndexData.getBytes("UTF-8"));

                System.out.println("----------------------------------------------->key: " + myIndexKey);
                System.out.println("----------------------------------------------->values: " + myIndexData);

                if (myDatabase.putNoOverwrite(null, theKey, theData) != OperationStatus.SUCCESS) {
                    throw new Exception("ERROR : DUPLICATE KEY, data can't be overwrite!!!");
                }
            } else {

                String dataString = new String(theData.getData(), "UTF-8");
                dataString = dataString + "#" + myIndexData;

                System.out.println("----------------------------------------------->key: " + myIndexKey);
                System.out.println("----------------------------------------------->values: " + dataString);

                theData = new DatabaseEntry(dataString.getBytes("UTF-8"));
                if (myDatabase.put(null, theKey, theData) != OperationStatus.SUCCESS) {
                    throw new Exception("ERROR : INSERT index non unique!!!");
                }
            }
        } catch (DatabaseException de) {
            throw new Exception("Error DatabaseException insert index: " + de);
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
    }

    public void insertIndexUniqueEntry(String attrIndexStructure, String myIndexKey, String myIndexData, String dbName, String tbName) throws Exception {
        System.out.println("dbmsimpl.TestBerkley.insertIndexUniqueEntry()" + "attr" + attrIndexStructure + " tb:" + tbName + " db:" + dbName);

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName + "\\" + attrIndexStructure;

        if (!(new File(filename).exists())) {
            new File(filename).mkdirs();
        }

        if (!(new File(filename).exists())) {
            throw new Exception("File path " + filename + " can't be created!");
        }

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

            String key = myIndexKey;
            String values = myIndexData;

            System.out.println("----------------------------------------------->key: " + key);
            System.out.println("----------------------------------------------->values: " + values);

            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theData = new DatabaseEntry(values.getBytes("UTF-8"));
            if (myDatabase.putNoOverwrite(null, theKey, theData) != OperationStatus.SUCCESS) {
                throw new Exception("ERROR : DUPLICATE KEY, data can't be overwrite!!!");
            }
        } catch (DatabaseException de) {
            throw new Exception("Error DatabaseException insert index: " + de);
        } finally {
            try {
                if (myCursor != null) {
                    myCursor.close();
                }
            } catch (DatabaseException dbe) {
                throw new Exception("Error closing cursor index: " + dbe.toString());
            }
        }
        if (myDatabase != null) {
            myDatabase.close();
        }
        if (myDbEnvironment != null) {
            myDbEnvironment.close();
        }
    }

    public void loadDataFromIndex(String attrIndexStructure, String dbName, String tbName) throws Exception {

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName + "\\" + attrIndexStructure;

        if (!(new File(filename).exists())) {
            throw new Exception("NO INDEX, File path " + filename + " don't exist!");
        }

        Environment myDbEnvironment = null;
        com.sleepycat.je.Database myDatabase = null;
        Cursor myCursor = null;
        try {
            myDbEnvironment = new Environment(new File(filename), null);
            myDatabase = myDbEnvironment.openDatabase(null, attrIndexStructure, null);
            myCursor = myDatabase.openCursor(null, null);

            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            while (myCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                String keyString = new String(foundKey.getData(), "UTF-8");
                String dataString = new String(foundData.getData(), "UTF-8");

                System.out.println("Key | Data : " + keyString + " | "
                        + dataString + "");
            }

        } catch (DatabaseException dbe) {
            // throw exception
            dbe.printStackTrace();
            throw new Exception("ERROR accessing database." + dbe.getMessage());
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
                throw new Exception("ERROR in close: " + dbe.toString());
            }
        }
    }

    public List<String> loadDataFromIndexInList(String attrIndexStructure, String tbName, String dbName) throws Exception {

        List<String> list = new ArrayList<String>();

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName + "\\" + attrIndexStructure;

        if (!(new File(filename).exists())) {
            throw new Exception("NO INDEX, File path " + filename + " don't exist!");
        }

        Environment myDbEnvironment = null;
        Database myDatabase = null;
        Cursor myCursor = null;
        try {
            myDbEnvironment = new Environment(new File(filename), null);
            myDatabase = myDbEnvironment.openDatabase(null, attrIndexStructure, null);
            myCursor = myDatabase.openCursor(null, null);

            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            while (myCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                String keyString = new String(foundKey.getData(), "UTF-8");
                String dataString = new String(foundData.getData(), "UTF-8");

                String elem = keyString + "@" + dataString;
                list.add(elem);
            }

        } catch (DatabaseException dbe) {
            // throw exception
            dbe.printStackTrace();
            throw new Exception("ERROR accessing database." + dbe.getMessage());
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
                throw new Exception("ERROR in close: " + dbe.toString());
            }
        }
        return list;
    }

    public String[][] loadDataFromTable(String dbName, String tbName) throws Exception {

        //Prima coloana are atributele apoi sunt calorile;
        String[][] matrix = new String[100][100];

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

            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            int k = 1;

            while (myCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                String keyString = new String(foundKey.getData(), "UTF-8");
                String dataString = new String(foundData.getData(), "UTF-8");

                System.out.println("Key | Data : " + keyString + " | "
                        + dataString + "");

                //Adauga prima linie numele attributelor
                List<Attribute> attrStructList = repoXML.getAttributesOfTableOfDb(dbName, tbName);
                for (int i = 0; i < attrStructList.size(); i++) {
                    if (!attrStructList.get(i).isPrimaryKey()) {
                        matrix[0][i] = attrStructList.get(i).getAttrName();
                    } else {
                        matrix[0][i] = attrStructList.get(i).getAttrName();
                    }
                }
                //Adauga pe urmatoarea linie valoarea attributelor
                for (int i = 0; i < attrStructList.size(); i++) {
                    if (!attrStructList.get(i).isPrimaryKey()) {
                        String attrValue = repoXML.getAttributeValueFromDataValue(attrStructList.get(i).getAttrName(), dataString, dbName, tbName);
                        matrix[k][i] = attrValue;
                    } else {
                        matrix[k][i] = keyString;
                    }
                }
                k++;
            }
            return matrix;
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
        return matrix;
    }

    public List<String> loadDataFromTableInList(String tbName, String dbName) throws Exception {

        List<String> list = new ArrayList<String>();

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName;

        if (!(new File(filename).exists())) {
            throw new Exception("File path " + filename + " don't exist!");
        }

        Environment myDbEnvironment = null;
        Database myDatabase = null;
        Cursor myCursor = null;
        try {
            myDbEnvironment = new Environment(new File(filename), null);
            myDatabase = myDbEnvironment.openDatabase(null, tbName, null);
            myCursor = myDatabase.openCursor(null, null);

            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            while (myCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                String keyString = new String(foundKey.getData(), "UTF-8");
                String dataString = new String(foundData.getData(), "UTF-8");

                String elem = keyString + "@" + dataString;
                list.add(elem);

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
        return list;
    }

    public List<String> select(List<SelectItem> forSelect) throws Exception {
        List<String> result = new ArrayList<String>();
        List<String> filtered = new ArrayList<String>();
        List<String> filteredResult = new ArrayList<String>();
        List<String> groupByResult = new ArrayList<String>();
        List<String> havingResult = new ArrayList<String>();
        List<String> filteredResultFinal = new ArrayList<String>();
        List<String> sortList = new ArrayList<String>();
        List<String> filters = new ArrayList<String>();
        for (int i = 0; i < forSelect.size(); i++) {
            //if(forSelect.get(i).getSortType()!="")
            //  sortList.add(forSelect.get(i).getSortOrder()+"@"+forSelect.get(i).getSortType()+"@"
            //        +forSelect.get(i).getColumn()+"@"+forSelect.get(i).getDbName()+"@"+forSelect.get(i).getTbName());
            if (forSelect.get(i).getFilter() != "") {
                filters.add(forSelect.get(i).getFilter());
                if (forSelect.get(i).getFilter().startsWith("<>")) {
                    filtered = differentFrom(forSelect.get(i).getAttr(), forSelect.get(i).getTbName(),
                            forSelect.get(i).getDbName(), forSelect.get(i).getFilter());
                    result.addAll(filtered);
                } else if (forSelect.get(i).getFilter().startsWith("<")) {
                    filtered = lessThan(forSelect.get(i).getAttr(), forSelect.get(i).getTbName(),
                            forSelect.get(i).getDbName(), forSelect.get(i).getFilter());
                    result.addAll(filtered);
                } else if (forSelect.get(i).getFilter().startsWith(">")) {
                    filtered = greaterThan(forSelect.get(i).getAttr(), forSelect.get(i).getTbName(),
                            forSelect.get(i).getDbName(), forSelect.get(i).getFilter());
                    result.addAll(filtered);
                } else if (forSelect.get(i).getFilter().startsWith("=")) {
                    filtered = equalWith(forSelect.get(i).getAttr(), forSelect.get(i).getTbName(),
                            forSelect.get(i).getDbName(), forSelect.get(i).getFilter());
                    result.addAll(filtered);
                } else if (forSelect.get(i).getFilter().startsWith("<=")) {
                    filtered = lessThanOrEqualWith(forSelect.get(i).getAttr(), forSelect.get(i).getTbName(),
                            forSelect.get(i).getDbName(), forSelect.get(i).getFilter());
                    result.addAll(filtered);
                } else if (forSelect.get(i).getFilter().startsWith(">=")) {
                    filtered = greaterThanOrEqualWith(forSelect.get(i).getAttr(), forSelect.get(i).getTbName(),
                            forSelect.get(i).getDbName(), forSelect.get(i).getFilter());
                    result.addAll(filtered);
                } else if (forSelect.get(i).getFilter().startsWith("BETWEEN")) {
                    filtered = between(forSelect.get(i).getAttr(), forSelect.get(i).getTbName(),
                            forSelect.get(i).getDbName(), forSelect.get(i).getFilter());
                    result.addAll(filtered);
                }
            }
        }
        for (int i = 0; i < result.size(); i++) {
            String[] parts = result.get(i).split("@");
            String[] keys = parts[0].split("#");
            for (int j = 0; j < keys.length; j++) {
                filteredResult.add(getDataByKeyFromTable(keys[j], parts[2], parts[1]));
            }
        }

        filteredResult = finalFilter(forSelect, filteredResult);

        boolean agg = false;
        boolean minMax = false;
        Boolean have = false;

        for (int i = 0; i < forSelect.size(); i++) {
            if (forSelect.get(i).isDistinct()) {
                Set<String> hs = new HashSet<String>();
                for (int k = 0; k < filteredResult.size(); k++) {
                    String attr = repoXML.getAttributeValueFromDataValue(forSelect.get(i).getAttr(),
                            filteredResult.get(k), forSelect.get(i).getDbName(), forSelect.get(i).getTbName());
                    hs.add(attr);
                }
                filteredResult.clear();
                filteredResult.addAll(hs);
            } else if (forSelect.get(i).isGroupBy()) {
                agg = true;
                if (forSelect.get(i).getAggFunction().equals("SUM"))
                    groupByResult = sum(filteredResult, forSelect.get(i).getAttr(), forSelect.get(i).getAlias(),
                            forSelect.get(i).getAttrGroupBy(), forSelect.get(i).getDbName(),
                            forSelect.get(i).getTbName());
                else if (forSelect.get(i).getAggFunction().equals("AVG"))
                    groupByResult = avg(filteredResult, forSelect.get(i).getAttr(), forSelect.get(i).getAlias(),
                            forSelect.get(i).getAttrGroupBy(), forSelect.get(i).getDbName(),
                            forSelect.get(i).getTbName());
                else if (forSelect.get(i).getAggFunction().equals("COUNT"))
                    groupByResult = count(filteredResult, forSelect.get(i).getAttr(), forSelect.get(i).getAlias(),
                            forSelect.get(i).getAttrGroupBy(), forSelect.get(i).getDbName(),
                            forSelect.get(i).getTbName());
                else if (forSelect.get(i).getAggFunction().equals("MIN")) {
                    minMax = true;
                    groupByResult = min(filteredResult, forSelect.get(i).getAttr(), forSelect.get(i).getDbName(),
                            forSelect.get(i).getTbName());
                } else if (forSelect.get(i).getAggFunction().equals("MAX")) {
                    minMax = true;
                    groupByResult = max(filteredResult, forSelect.get(i).getAttr(), forSelect.get(i).getDbName(),
                            forSelect.get(i).getTbName());
                } else if (forSelect.get(i).getAggFunction().equals("FIRST")) {
                    minMax = true;
                    groupByResult = min(filteredResult, forSelect.get(i).getAttr(), forSelect.get(i).getDbName(),
                            forSelect.get(i).getTbName());
                } else if (forSelect.get(i).getAggFunction().equals("LAST")) {
                    minMax = true;
                    groupByResult = max(filteredResult, forSelect.get(i).getAttr(), forSelect.get(i).getDbName(),
                            forSelect.get(i).getTbName());
                }
            }
        }

        if (agg) {
            for (int i = 0; i < forSelect.size(); i++) {
                if (forSelect.get(i).isHaving()) {
                    have = true;
                    havingResult = having(groupByResult, forSelect.get(i).getAttrGroupBy(),
                            forSelect.get(i).getCondition(), forSelect.get(i).getDbName(),
                            forSelect.get(i).getTbName(), minMax);
                }
            }

        }
        if (have)
            filteredResultFinal.addAll(havingResult);
        if (agg && !have)
            filteredResultFinal.addAll(groupByResult);
        if (!agg)
            filteredResultFinal.addAll(filteredResult);

        return filteredResultFinal;
    }

    public List<String> finalFilter(List<SelectItem> forSelect, List<String> list) {
        String column = "";
        String filter = "";
        for (int i = 0; i < forSelect.size(); i++) {
            if (forSelect.get(i).getFilter() != "") {
                column = forSelect.get(i).getAttr();
                System.out.println("column=============" + column);
                filter = forSelect.get(i).getFilter();
                System.out.println("filter============" + filter);
                for (int j = 0; j < list.size(); j++) {
                    if (filter.startsWith("=")) {
                        if (!repoXML.getAttributeValueFromDataValue(column, list.get(j),
                                forSelect.get(i).getDbName(), forSelect.get(i).getTbName()).equals(filter.substring(1))) {
                            list.remove(j);
                            j--;
                        }
                    } else if (filter.startsWith("<>")) {
                        if (repoXML.getAttributeValueFromDataValue(column, list.get(j),
                                forSelect.get(i).getDbName(), forSelect.get(i).getTbName()).equals(filter.substring(2))) {
                            list.remove(j);
                            j--;
                        }
                    } else if (filter.startsWith("<")) {
                        if (Integer.parseInt(repoXML.getAttributeValueFromDataValue(column, list.get(j),
                                forSelect.get(i).getDbName(), forSelect.get(i).getTbName())) >= Integer.parseInt(filter.substring(1))) {
                            list.remove(j);
                            j--;
                        }
                    } else if (filter.startsWith(">")) {
                        if (Integer.parseInt(repoXML.getAttributeValueFromDataValue(column, list.get(j),
                                forSelect.get(i).getDbName(), forSelect.get(i).getTbName())) <= Integer.parseInt(filter.substring(1))) {
                            list.remove(j);
                            j--;
                        }
                    } else if (filter.startsWith("<=")) {
                        if (Integer.parseInt(repoXML.getAttributeValueFromDataValue(column, list.get(j),
                                forSelect.get(i).getDbName(), forSelect.get(i).getTbName())) > Integer.parseInt(filter.substring(2))) {
                            list.remove(j);
                            j--;
                        }
                    } else if (filter.startsWith(">=")) {
                        if (Integer.parseInt(repoXML.getAttributeValueFromDataValue(column, list.get(j),
                                forSelect.get(i).getDbName(), forSelect.get(i).getTbName())) < Integer.parseInt(filter.substring(2))) {
                            list.remove(j);
                            j--;
                        }
                    } else if (filter.startsWith("BETWEEN")) {
                        String[] cond = filter.split(" ");
                        if (Integer.parseInt(repoXML.getAttributeValueFromDataValue(column, list.get(j),
                                forSelect.get(i).getDbName(), forSelect.get(i).getTbName())) < Integer.parseInt(cond[1])
                                || Integer.parseInt(repoXML.getAttributeValueFromDataValue(column, list.get(j),
                                forSelect.get(i).getDbName(), forSelect.get(i).getTbName())) > Integer.parseInt(cond[3])) {
                            list.remove(j);
                            j--;
                        }
                    }
                }
            }
        }
        Set<String> hs = new HashSet<String>();
        hs.addAll(list);
        list.clear();
        list.addAll(hs);
        return list;
    }

    public List<String> lessThan(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> data = new ArrayList<String>();
        List<String> filtered = new ArrayList<String>();
        data = loadDataFromIndexInList(column, tbName, dbName);
        if (data.size() == 0) {
            data = loadDataFromTableInList(tbName, dbName);
        }
        for (int i = 0; i < data.size(); i++) {
            String[] key = data.get(i).split("@");
            String[] cond = filter.split("<");
            if (Integer.parseInt(key[0]) < Integer.parseInt(cond[1])) //filtered.add(key[0]);
            {
                filtered.add(key[1] + "@" + tbName + "@" + dbName);
            }
        }
        return filtered;
    }

    public List<String> greaterThan(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> data = new ArrayList<String>();
        List<String> filtered = new ArrayList<String>();
        data = loadDataFromIndexInList(column, tbName, dbName);
        if (data.size() == 0) {
            data = loadDataFromTableInList(tbName, dbName);
        }
        for (int i = 0; i < data.size(); i++) {
            String[] key = data.get(i).split("@");
            String[] cond = filter.split(">");
            if (Integer.parseInt(key[0]) > Integer.parseInt(cond[1])) //filtered.add(key[0]);
            {
                filtered.add(key[1] + "@" + tbName + "@" + dbName);
            }
        }
        return filtered;
    }

    public List<String> equalWith(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> data = new ArrayList<String>();
        List<String> filtered = new ArrayList<String>();
        data = loadDataFromIndexInList(column, tbName, dbName);
        if (data.size() == 0) {
            data = loadDataFromTableInList(tbName, dbName);
        }
        for (int i = 0; i < data.size(); i++) {
            String[] key = data.get(i).split("@");
            String[] cond = filter.split("=");
            //if(Integer.parseInt(key[0])==Integer.parseInt(cond[1]))
            if (key[0].equals(cond[1])) //filtered.add(key[0]);
            {
                filtered.add(key[1] + "@" + tbName + "@" + dbName);
            }
        }
        return filtered;
    }

    public List<String> lessThanOrEqualWith(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> data = new ArrayList<String>();
        List<String> filtered = new ArrayList<String>();
        data = loadDataFromIndexInList(column, tbName, dbName);
        if (data.size() == 0) {
            data = loadDataFromTableInList(tbName, dbName);
        }
        for (int i = 0; i < data.size(); i++) {
            String[] key = data.get(i).split("@");
            String[] cond = filter.split("<=");
            if (Integer.parseInt(key[0]) <= Integer.parseInt(cond[1])) //filtered.add(key[0]);
            {
                filtered.add(key[1] + "@" + tbName + "@" + dbName);
            }
        }
        return filtered;
    }

    public List<String> greaterThanOrEqualWith(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> data = new ArrayList<String>();
        List<String> filtered = new ArrayList<String>();
        data = loadDataFromIndexInList(column, tbName, dbName);
        if (data.size() == 0) {
            data = loadDataFromTableInList(tbName, dbName);
        }
        for (int i = 0; i < data.size(); i++) {
            String[] key = data.get(i).split("@");
            String[] cond = filter.split(">=");
            if (Integer.parseInt(key[0]) >= Integer.parseInt(cond[1])) //filtered.add(key[0]);
            {
                filtered.add(key[1] + "@" + tbName + "@" + dbName);
            }
        }
        return filtered;
    }

    public List<String> between(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> data = new ArrayList<String>();
        List<String> filtered = new ArrayList<String>();
        data = loadDataFromIndexInList(column, tbName, dbName);
        if (data.size() == 0) {
            data = loadDataFromTableInList(tbName, dbName);
        }
        for (int i = 0; i < data.size(); i++) {
            String[] key = data.get(i).split("@");
            String[] cond = filter.split("BETWEEN");
            String[] cond2 = cond[1].split("AND");
            if (Integer.parseInt(key[0]) >= Integer.parseInt((cond2[0]).trim()) && Integer.parseInt(key[0]) <= Integer.parseInt(cond2[1].trim())) //filtered.add(key[0]);
            {
                filtered.add(key[1] + "@" + tbName + "@" + dbName);
            }
        }
        return filtered;
    }

    public List<String> differentFrom(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> data = new ArrayList<String>();
        List<String> filtered = new ArrayList<String>();
        data = loadDataFromIndexInList(column, tbName, dbName);
        if (data.size() == 0) {
            data = loadDataFromTableInList(tbName, dbName);
        }
        for (int i = 0; i < data.size(); i++) {
            String[] key = data.get(i).split("@");
            String[] cond = filter.split("<>");
            if (!key[0].equals(cond[1])) //filtered.add(key[0]);
            {
                filtered.add(key[1] + "@" + tbName + "@" + dbName);
            }
        }
        return filtered;
    }

    public List<String> sortAscending(List<String> filtered) {
        Collections.sort(filtered);
        return filtered;
    }

    public List<String> sortDescending(List<String> filtered) {
        Collections.sort(filtered, Collections.reverseOrder());
        return filtered;
    }

    public List<String> sort(List<String> list, String attribute, String order, String dbName, String tbName) {
        List<String> fields = new ArrayList<String>();
        List<String> sorted = new ArrayList<String>();
        for (int i = 0; i < list.size(); i++) {
            String field = repoXML.getAttributeValueFromDataValue(attribute, list.get(i), dbName, tbName);
            field = field + " " + i;
            fields.add(field);
        }
        if (order.equals("desc"))
            sortDescending(fields);
        else
            sortAscending(fields);
        for (int i = 0; i < fields.size(); i++) {
            String[] parts = fields.get(i).split(" ");
            for (int j = 0; j < list.size(); j++) {
                if (j == Integer.parseInt(parts[1])) {
                    sorted.add(list.get(j));
                }
            }
        }
        return sorted;
    }

    public List<String> sum(List<String> data, String attribute, String alias, String groupBy,
                            String dbName, String tbName) throws Exception {
        //List<String> data = new ArrayList<String>();
        //List<String> list = new ArrayList<String>();
        List<String> deduplicate = new ArrayList<String>();
        List<String> rez = new ArrayList<String>();
        List<Struct> struct = new ArrayList<Struct>();
        int sum = 0;
//        list = loadDataFromTableInList(tbName, dbName);
//        for(int i=0;i<list.size();i++){
//            System.out.println("paaaaaaa "+list.get(i));
//            String[] parts = list.get(i).split("@");
//            data.add(parts[1]);
//        }
        for (int i = 0; i < data.size(); i++) {
            String grBy = repoXML.getAttributeValueFromDataValue(groupBy, data.get(i), dbName, tbName);
            if (!deduplicate.contains(grBy)) {
                deduplicate.add(grBy);
            }
        }
        for (int i = 0; i < deduplicate.size(); i++) {
            Struct str = new Struct();
            str.attr = deduplicate.get(i);
            str.sum = 0;
            struct.add(str);
        }
        for (int i = 0; i < data.size(); i++) {
            String attr = repoXML.getAttributeValueFromDataValue(attribute, data.get(i), dbName, tbName);
            String grBy = repoXML.getAttributeValueFromDataValue(groupBy, data.get(i), dbName, tbName);
            int poz = 0;
            for (int j = 0; j < struct.size(); j++) {
                if (grBy.equals(struct.get(j).attr))
                    poz = j;
            }
            struct.get(poz).sum = struct.get(poz).sum + Integer.parseInt(attr);
        }
        for (int i = 0; i < struct.size(); i++) {
            rez.add(struct.get(i).attr + "#" + struct.get(i).sum);
        }
        return rez;
    }

    public List<String> avg(List<String> data, String attribute, String alias, String groupBy,
                            String dbName, String tbName) throws Exception {
//        List<String> data = new ArrayList<String>();
//        List<String> list = new ArrayList<String>();
        List<String> deduplicate = new ArrayList<String>();
        List<String> rez = new ArrayList<String>();
        List<Struct3> struct = new ArrayList<Struct3>();
//        list = loadDataFromTableInList(tbName, dbName);
//        for(int i=0;i<list.size();i++){
//            System.out.println("paaaaaaa "+list.get(i));
//            String[] parts = list.get(i).split("@");
//            data.add(parts[1]);
//        }
        for (int i = 0; i < data.size(); i++) {
            String grBy = repoXML.getAttributeValueFromDataValue(groupBy, data.get(i), dbName, tbName);
            if (!deduplicate.contains(grBy)) {
                deduplicate.add(grBy);
            }
        }
        for (int i = 0; i < deduplicate.size(); i++) {
            Struct3 str = new Struct3();
            str.attr = deduplicate.get(i);
            str.sum = 0;
            str.nr = 0;
            struct.add(str);
        }
        for (int i = 0; i < data.size(); i++) {
            String attr = repoXML.getAttributeValueFromDataValue(attribute, data.get(i), dbName, tbName);
            String grBy = repoXML.getAttributeValueFromDataValue(groupBy, data.get(i), dbName, tbName);
            int poz = 0;
            for (int j = 0; j < struct.size(); j++) {
                if (grBy.equals(struct.get(j).attr))
                    poz = j;
            }
            struct.get(poz).sum = struct.get(poz).sum + Integer.parseInt(attr);
            struct.get(poz).nr = struct.get(poz).nr + 1;
            struct.get(poz).avg = struct.get(poz).sum / struct.get(poz).nr;
        }
        for (int i = 0; i < struct.size(); i++) {
            rez.add(struct.get(i).attr + "#" + struct.get(i).avg);
        }
        return rez;
    }

    public List<String> count(List<String> data, String attribute, String alias, String groupBy,
                              String dbName, String tbName) throws Exception {
//        List<String> data = new ArrayList<String>();
//        List<String> list = new ArrayList<String>();
        List<String> deduplicate = new ArrayList<String>();
        List<String> rez = new ArrayList<String>();
        List<Struct2> struct = new ArrayList<Struct2>();
//        list = loadDataFromTableInList(tbName, dbName);
//        for(int i=0;i<list.size();i++){
//            System.out.println("paaaaaaa "+list.get(i));
//            String[] parts = list.get(i).split("@");
//            data.add(parts[1]);
//        }
        for (int i = 0; i < data.size(); i++) {
            String grBy = repoXML.getAttributeValueFromDataValue(groupBy, data.get(i), dbName, tbName);
            if (!deduplicate.contains(grBy)) {
                deduplicate.add(grBy);
            }
        }
        for (int i = 0; i < deduplicate.size(); i++) {
            Struct2 str = new Struct2();
            str.attr = deduplicate.get(i);
            str.nr = 0;
            struct.add(str);
        }
        for (int i = 0; i < data.size(); i++) {
            String grBy = repoXML.getAttributeValueFromDataValue(groupBy, data.get(i), dbName, tbName);
            int poz = 0;
            for (int j = 0; j < struct.size(); j++) {
                if (grBy.equals(struct.get(j).attr))
                    poz = j;
            }
            struct.get(poz).nr = struct.get(poz).nr + 1;
        }
        for (int i = 0; i < struct.size(); i++) {
            rez.add(struct.get(i).attr + "#" + struct.get(i).nr);
        }
        return rez;
    }

    public List<String> min(List<String> data, String attribute, String dbName, String tbName) throws Exception {
        List<String> sort = new ArrayList<String>();
        List<String> min = new ArrayList<String>();
        sort = sort(data, attribute, "", dbName, tbName);
        min.add(sort.get(0));
        return min;
    }

    public List<String> max(List<String> data, String attribute, String dbName, String tbName) throws Exception {
        List<String> sort = new ArrayList<String>();
        List<String> max = new ArrayList<String>();
        sort = sort(data, attribute, "", dbName, tbName);
        max.add(sort.get(sort.size() - 1));
        return max;
    }

    public List<String> first(List<String> data, String attribute, String dbName, String tbName) throws Exception {
        List<String> first = new ArrayList<String>();
        first.add(data.get(0));
        return first;
    }

    public List<String> last(List<String> data, String attribute, String dbName, String tbName) throws Exception {
        List<String> last = new ArrayList<String>();
        last.add(data.get(data.size() - 1));
        return last;
    }

    public List<String> having(List<String> list, String attribute, String cond, String dbName, String tbName, boolean minMax) {
        List<String> result = new ArrayList<String>();
        for (int i = 0; i < list.size(); i++) {
            System.out.println("-------" + list.get(i));
            String value = "";
            if (minMax)
                value = repoXML.getAttributeValueFromDataValue(attribute, list.get(i), dbName, tbName);
            else {
                String[] parts = list.get(i).split("#");
                value = parts[1];
            }
            if (cond.startsWith("=")) {
                if (value.equals(cond.substring(1)))
                    result.add(list.get(i));
            } else if (cond.startsWith("<>")) {
                if (!value.equals(cond.substring(2)))
                    result.add(list.get(i));
            } else if (cond.startsWith("<=")) {
                if (Integer.parseInt(value) <= (Integer.parseInt(cond.substring(2))))
                    result.add(list.get(i));
            } else if (cond.startsWith(">=")) {
                if (Integer.parseInt(value) >= (Integer.parseInt(cond.substring(2))))
                    result.add(list.get(i));
            } else if (cond.startsWith("<")) {
                if (Integer.parseInt(value) < (Integer.parseInt(cond.substring(1))))
                    result.add(list.get(i));
            } else if (cond.startsWith(">")) {
                if (Integer.parseInt(value) > (Integer.parseInt(cond.substring(1))))
                    result.add(list.get(i));
            } else if (cond.startsWith("BETWEEN")) {
                String[] elems = cond.split(" ");
                if (Integer.parseInt(value) >= (Integer.parseInt(elems[1])) && Integer.parseInt(value) <= (Integer.parseInt(elems[3])))
                    result.add(list.get(i));
            }
        }
        return result;
    }

    public List<String> union(List<String> listA, List<String> listB) {
        Set<String> hs = new HashSet<String>();
        List<String> unionList = new ArrayList<String>();
        hs.addAll(listA);
        hs.addAll(listB);
        unionList.addAll(hs);
        return unionList;
    }

    public List<String> intersect(List<String> listA, List<String> listB) {
        List<String> intersectList = new ArrayList<String>();
        for (int i = 0; i < listA.size(); i++)
            if (listB.contains(listA.get(i)))
                intersectList.add(listA.get(i));
        return intersectList;
    }

    public List<String> except(List<String> listA, List<String> listB) {
        List<String> exceptList = new ArrayList<String>();
        for (int i = 0; i < listA.size(); i++)
            if (!listB.contains(listA.get(i)))
                exceptList.add(listA.get(i));
        return exceptList;
    }

    public String innerJoinIndexNestedLoop(String dbName, String tbName1, String tbName2, String attr1, String attr2) throws Exception {
        String tbNameJoin = createTableJoin(dbName, tbName1, tbName2, attr1, attr2);

        String auxTable = tbName1;
        String auxAttr = attr1;

        String[] tableRef = tbNameJoin.split("_");
        if (tableRef[0].equals(tbName1)) {
            tbName1 = tbName2;
            attr1 = attr2;
            tbName2 = auxTable;
            attr2 = auxAttr;
        }
        insertDataTableJoinT1(dbName, tbNameJoin, tbName1, tbName2, attr1, attr2);
        return tbNameJoin;
    }

    public String innerJoinSortMerge(String dbName, String tbName1, String tbName2, String attr1, String attr2) throws Exception {
        String tbNameJoin = createTableJoin(dbName, tbName1, tbName2, attr1, attr2);

        String auxTable = tbName1;
        String auxAttr = attr1;

        String[] tableRef = tbNameJoin.split("_");
        if (tableRef[0].equals(tbName1)) {
            tbName1 = tbName2;
            attr1 = attr2;
            tbName2 = auxTable;
            attr2 = auxAttr;
        }
        //Table 2 attribut reffere Table 1 attribut    Table1   Table4   id     angajat  => Table4_Table1
        loadDataFromTableJoinMerge(dbName, tbNameJoin, tbName1, tbName2, attr1, attr2);
        return tbNameJoin;
    }

    public String createTableJoin(String dbName, String tbName1, String tbName2, String attr1, String attr2) {
        String tbNameJoin = "";
        //attr1 refera attr2 sin tb2 si  avem index pe attr1 pt ref
        if (repoXML.isFkAttribute(attr1, dbName, tbName1)) {
            try {
                //Create table for result
                tbNameJoin = tbName1 + '_' + tbName2;
                repoXML.deleteTableFromDatabase(dbName, tbNameJoin);
                repoXML.createTable(dbName, tbNameJoin);

                for (Attribute a : repoXML.getAttributesOfTableOfDb(dbName, tbName1)) {

                    if (a.getAttrName().equals(attr1)) {
                        repoXML.addAttributeToTable(dbName, tbNameJoin, a.getAttrName(), a.getAttrType(), a.isPrimaryKey(), a.isUnique(), a.isNull(), "", "");

                    } else {
                        repoXML.addAttributeToTable(dbName, tbNameJoin, a.getAttrName(), a.getAttrType(), a.isPrimaryKey(), a.isUnique(), a.isNull(), a.getReferencedTable(), a.getReferencedAttribute());
                    }
                }
                for (Attribute a : repoXML.getAttributesOfTableOfDb(dbName, tbName2)) {

                    if (!a.isPrimaryKey()) {
                        repoXML.addAttributeToTable(dbName, tbNameJoin, a.getAttrName(), a.getAttrType(), false, false, a.isNull(), a.getReferencedTable(), a.getReferencedAttribute());

                    }
                }

            } catch (Exception ex) {
                Logger.getLogger(RepoBerekeley.class
                        .getName()).log(Level.SEVERE, null, ex);
            }

        } else //attr2 refera attr1 sin tb1 si  avem index pe attr2 pt ref
        {
            if (repoXML.isFkAttribute(attr2, dbName, tbName2)) {
                try {
                    //Create table for result
                    tbNameJoin = tbName2 + '_' + tbName1;
                    repoXML.deleteTableFromDatabase(dbName, tbNameJoin);
                    repoXML.createTable(dbName, tbNameJoin);

                    for (Attribute a : repoXML.getAttributesOfTableOfDb(dbName, tbName2)) {
                        if (a.getAttrName().equals(attr2)) {
                            repoXML.addAttributeToTable(dbName, tbNameJoin, a.getAttrName(), a.getAttrType(), a.isPrimaryKey(), a.isUnique(), a.isNull(), "", "");

                        } else {
                            repoXML.addAttributeToTable(dbName, tbNameJoin, a.getAttrName(), a.getAttrType(), a.isPrimaryKey(), a.isUnique(), a.isNull(), a.getReferencedTable(), a.getReferencedAttribute());
                        }
                    }

                    for (Attribute a : repoXML.getAttributesOfTableOfDb(dbName, tbName1)) {

                        if (!a.isPrimaryKey()) {
                            repoXML.addAttributeToTable(dbName, tbNameJoin, a.getAttrName(), a.getAttrType(), false, false, a.isNull(), a.getReferencedTable(), a.getReferencedAttribute());

                        }
                    }

                } catch (Exception ex) {
                    Logger.getLogger(RepoBerekeley.class
                            .getName()).log(Level.SEVERE, null, ex);
                }

            }
        }

        return tbNameJoin;

    }

    public void loadDataFromTableJoinMerge(String dbName, String tbNameJoin, String tbName1, String tbName2, String attr1, String attr2) throws Exception {

        //Open File for table with pk
        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName1;

        if (!(new File(filename).exists())) {
            throw new Exception("File path " + filename + " don't exist!");
        }

        Environment myDbEnvironment = null;
        com.sleepycat.je.Database myDatabase = null;
        Cursor myCursor = null;

        //Open File for Index Table with reference to table 1
        String filename2 = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename2 = filename2 + dbName + "\\" + tbName2 + "\\" + attr2;

        if (!(new File(filename2).exists())) {
            throw new Exception("NO INDEX, File path " + filename2 + " don't exist!");
        }

        Environment myDbEnvironment2 = null;
        com.sleepycat.je.Database myDatabase2 = null;
        Cursor myCursor2 = null;
        try {
            //For table 1
            myDbEnvironment = new Environment(new File(filename), null);
            myDatabase = myDbEnvironment.openDatabase(null, tbName1, null);
            myCursor = myDatabase.openCursor(null, null);

            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            //For index table 1
            myDbEnvironment2 = new Environment(new File(filename2), null);
            myDatabase2 = myDbEnvironment2.openDatabase(null, attr2, null);
            myCursor2 = myDatabase2.openCursor(null, null);

            DatabaseEntry foundKeyIndex = new DatabaseEntry();
            DatabaseEntry foundDataIndex = new DatabaseEntry();

            while (myCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS
                    && myCursor2.getNext(foundKeyIndex, foundDataIndex, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                String keyStringTb1 = new String(foundKey.getData(), "UTF-8");
                String dataStringTb1 = new String(foundData.getData(), "UTF-8");

                String keyStringIndex = new String(foundKeyIndex.getData(), "UTF-8");
                String dataStringIndex = new String(foundDataIndex.getData(), "UTF-8");

                while (Integer.parseInt(keyStringTb1) < Integer.parseInt(keyStringIndex)) {
                    myCursor.getNext(foundKey, foundData, LockMode.DEFAULT);

                    keyStringTb1 = new String(foundKey.getData(), "UTF-8");
                    dataStringTb1 = new String(foundData.getData(), "UTF-8");

                }
//                while (Integer.parseInt(keyStringTb1) < Integer.parseInt(keyStringIndex)) {
//                    if (myCursor2.getNext(foundKeyIndex, foundDataIndex, LockMode.DEFAULT) == OperationStatus.NOTFOUND) {
//                        break;
//                    } else {
//                        keyStringIndex = new String(foundKeyIndex.getData(), "UTF-8");
//                        dataStringIndex = new String(foundDataIndex.getData(), "UTF-8");
//                    }
//                }

                if (keyStringTb1.equals(keyStringIndex)) {
                    //Daca key sunt egale adaugam in yabelul de join
                    if (!dataStringIndex.equals("")) {
                        String[] keysTable2 = dataStringIndex.split("#");

                        for (String keyTb2 : keysTable2) {
                            String valueTable2 = getDataByKeyFromTable(keyTb2, dbName, tbName2);

                            List<String> attrJoin = new ArrayList<String>();

                            for (Attribute a : repoXML.getAttributesOfTableOfDb(dbName, tbName2)) {
                                if (!a.isPrimaryKey()) {
                                    attrJoin.add(repoXML.getAttributeValueFromDataValue(a.getAttrName(), valueTable2, dbName, tbName2));
                                } else {
                                    attrJoin.add(keyTb2);
                                }
                            }

                            for (Attribute a : repoXML.getAttributesOfTableOfDb(dbName, tbName1)) {
                                if (!a.isPrimaryKey()) {
                                    attrJoin.add(repoXML.getAttributeValueFromDataValue(a.getAttrName(), dataStringTb1, dbName, tbName1));
                                }
                            }
                            insertEntry(attrJoin, dbName, tbNameJoin);
                        }
                    }
                }
            }

        } catch (DatabaseException dbe) {
            // throw exception
            System.err.println("Error accessing database." + dbe);
        } finally {
            try {
                if (myCursor != null || myCursor2 != null) {
                    myCursor.close();
                    myCursor2.close();
                }
                if (myDatabase != null || myDatabase2 != null) {
                    myDatabase.close();
                    myDatabase2.close();
                }
                if (myDbEnvironment != null || myDbEnvironment2 != null) {
                    myDbEnvironment.close();
                    myDbEnvironment2.close();
                }
            } catch (DatabaseException dbe) {
                System.err.println("Error in close: " + dbe.toString());
            }
        }
    }

    public void insertDataTableJoinT1(String dbName, String tbNameJoin, String tbName1, String tbName2, String attr1, String attr2) throws Exception {

        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName1;

        if (!(new File(filename).exists())) {
            throw new Exception("File path " + filename + " don't exist!");
        }

        Environment myDbEnvironment = null;
        com.sleepycat.je.Database myDatabase = null;
        Cursor myCursor = null;
        try {
            myDbEnvironment = new Environment(new File(filename), null);
            myDatabase = myDbEnvironment.openDatabase(null, tbName1, null);
            myCursor = myDatabase.openCursor(null, null);

            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            while (myCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                String keyStringTb1 = new String(foundKey.getData(), "UTF-8");
                String dataStringTb1 = new String(foundData.getData(), "UTF-8");

                System.out.println("tb:" + tbName1 + " Key | Data  : " + keyStringTb1 + " | "
                        + dataStringTb1 + "");
                String listIdTb2 = getDataByKeyIndexJoinTable2(keyStringTb1, attr2, dbName, tbName2);
                // if list Id has no value mean that is not refered and we don't need this entry from Table1

                if (!listIdTb2.equals("")) {
                    String[] keysTable2 = listIdTb2.split("#");

                    for (String keyTb2 : keysTable2) {
                        String valueTable2 = getDataByKeyFromTable(keyTb2, dbName, tbName2);

                        List<String> attrJoin = new ArrayList<String>();

                        for (Attribute a : repoXML.getAttributesOfTableOfDb(dbName, tbName2)) {
                            if (!a.isPrimaryKey()) {
                                attrJoin.add(repoXML.getAttributeValueFromDataValue(a.getAttrName(), valueTable2, dbName, tbName2));
                            } else {
                                attrJoin.add(keyTb2);
                            }
                        }

                        for (Attribute a : repoXML.getAttributesOfTableOfDb(dbName, tbName1)) {
                            if (!a.isPrimaryKey()) {
                                attrJoin.add(repoXML.getAttributeValueFromDataValue(a.getAttrName(), dataStringTb1, dbName, tbName1));
                            }
                        }

                        insertEntry(attrJoin, dbName, tbNameJoin);
                    }

                }

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
    }

    public String getDataByKeyIndexJoinTable2(String keyEntryValue, String attrIndexStructure, String dbName, String tbName) throws Exception {
        String filename = "C:\\NetBeans\\PROJECTS\\DBMSImpl\\";
        filename = filename + dbName + "\\" + tbName + "\\" + attrIndexStructure;

        if (!(new File(filename).exists())) {
            throw new Exception("NO INDEX, File path " + filename + " don't exist!");
        }

        Environment myDbEnvironment = null;
        com.sleepycat.je.Database myDatabase = null;
        Cursor myCursor = null;
        try {
            myDbEnvironment = new Environment(new File(filename), null);
            myDatabase = myDbEnvironment.openDatabase(null, attrIndexStructure, null);
            myCursor = myDatabase.openCursor(null, null);

            DatabaseEntry theKey = new DatabaseEntry(keyEntryValue.getBytes("UTF-8"));
            DatabaseEntry theData = new DatabaseEntry();

            if (myDatabase.get(null, theKey, theData, LockMode.DEFAULT) != OperationStatus.SUCCESS) {
                System.out.println("ERROR getDataByKeyFromTable : NO DATA WITH KEY = " + keyEntryValue + " Database:" + dbName + " Table:" + tbName + "!!!");
                return "";
            } else {
                return new String(theData.getData(), "UTF-8");
            }

        } catch (DatabaseException dbe) {
            // throw exception
            dbe.printStackTrace();
            throw new Exception("ERROR accessing database." + dbe.getMessage());
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
                throw new Exception("ERROR in close: " + dbe.toString());
            }
        }
    }

}
