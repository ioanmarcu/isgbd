package util;

import entity.*;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static util.XmlKeyword.*;

/**
 * @author I. Marcu
 */
public class DatabaseConnector {
    private static final Logger LOG = Logger.getLogger(DatabaseConnector.class.getName());

    public static String createDatabase(String databaseName) {

        Document loadedXml = loadXMLFromFile(DB_STRUCTURE_FILEPATH);
        if (loadedXml == null) {
            return "Error while creating database";
        }

        Element allDatabases = (Element) loadedXml.getElementsByTagName(ALL_DATABASES).item(0);
        Element database = loadedXml.createElement(DATABASE);
        try {
            updateElementProperties(loadedXml, database, DATABASE_NAME, databaseName);
        } catch (Exception e) {
            System.out.println("Error create");
        }

        //create Table Tag
        Element elTables = addChildToDocument(loadedXml, database, ALL_TABLES);
        allDatabases.appendChild(database);
        writeDocumentToFile(loadedXml, DB_STRUCTURE_FILEPATH);
        return "Successfully created database";
    }

    public static String dropDatabase(String databaseName) {

        Document loadedXml = loadXMLFromFile(DB_STRUCTURE_FILEPATH);
        if (loadedXml == null) {
            return "Error while creating database";
        }

        Element allDatabases = (Element) loadedXml.getElementsByTagName(ALL_DATABASES).item(0);
        NodeList nodes = allDatabases.getElementsByTagName(DATABASE);

        for (int i = 0; i < nodes.getLength(); i++) {
            Element database = (Element) nodes.item(i);
            String name = database.getAttribute(DATABASE_NAME);
            if (name.equals(databaseName)) {
                database.getParentNode().removeChild(database);
                break;
            }
        }
        writeDocumentToFile(loadedXml, DB_STRUCTURE_FILEPATH);
        return "Successfully deleted database " + databaseName;
    }

    private static Document loadXMLFromFile(String filepath) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        try {
            builder = factory.newDocumentBuilder();
            File file = new File(filepath);
            return builder.parse(file);
        } catch (ParserConfigurationException | SAXException | IOException | NullPointerException e) {
            LOG.log(Level.SEVERE, e.getMessage());
            return null;
        }
    }

    public static void writeDatabasesToXML(List<Database> databases) {
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.newDocument();


            Element rootElement = doc.createElement(ALL_DATABASES);
            doc.appendChild(rootElement);
            //create Database Tag
            for (Database db : databases) {
                Element database = doc.createElement(DATABASE);
                System.out.println(DATABASE);
                try {
                    updateElementProperties(doc, database, DATABASE_NAME, db.getName());
                } catch (Exception e) {
                    System.out.println("Error create");
                }
                rootElement.appendChild(database);

                //create Table Tag
                Element elTables = doc.createElement(ALL_TABLES);
                System.out.println(ALL_TABLES);
                database.appendChild(elTables);

                List<Table> tableList = db.getTables();
                if (!tableList.isEmpty()) {
                    for (Table aTableList : tableList) {
                        Element elTable = doc.createElement(TABLE);
                        try {
                            updateElementProperties(doc, elTable, TABLE_NAME, aTableList.getName());

                            //create Attribute Tag
                            Element elStructure = addChildToDocument(doc, elTable, TABLE_STRUCTURE);

                            //Create  primary Key Tag
                            List<String> pkList = aTableList.getPrimaryKeys();
                            if (!pkList.isEmpty()) {
                                Element elPrimaryKey = addChildToDocument(doc, elTable, PRIMARY_KEY);

                                for (String attr : pkList) {
                                    addChildNodeToParent(doc, elPrimaryKey, attr, PRIMARY_KEY_ATTRIBUTE);
                                }
                            }
                            //Create  unique Key Tag
                            List<String> uniquesList = aTableList.getUniqueKeys();
                            if (!uniquesList.isEmpty()) {
                                Element elUniqueKeys = addChildToDocument(doc, elTable, UNIQUE_KEYS);
                                for (String attr : uniquesList) {
                                    addChildNodeToParent(doc, elUniqueKeys, attr, UNIQUE_ATTRIBUTE);
                                }
                            }
                            //Create  foreignKeys Tag
                            List<ForeignKey> fkList = aTableList.getForeignKeys();
                            if (!fkList.isEmpty()) {
                                Element elForeignKeys = addChildToDocument(doc, elTable, ALL_FOREIGN_KEYS);
                                for (ForeignKey f : fkList) {
                                    if (!f.getReferencedAttribute().equals("")) {
                                        Element elForeignKey = addChildToDocument(doc, elForeignKeys, FOREIGN_KEY);
                                        addChildNodeToParent(doc, elForeignKey, f.getForeignKeyAttribute(), FOREIGN_KEY_ATTRIBUTE);

                                        //references tag inside ForengKey
                                        Element references = doc.createElement(TABLE_REFERENCES);
                                        System.out.println(TABLE_REFERENCES);
                                        elForeignKey.appendChild(references);
                                        addChildNodeToParent(doc, references, f.getReferencedTable(), REFERENCED_TABLE);
                                        addChildNodeToParent(doc, references, f.getReferencedAttribute(), REFERENCED_ATTRIBUTE);
                                    }
                                }
                            }
                            //Create  IndexFiles, IndexFile, IndexAttributes Tag
                            List<IndexFile> indexList = aTableList.getIndexFiles();
                            if (!indexList.isEmpty()) {
                                Element elIndexFiles = addChildToDocument(doc, elTable, ALL_INDEX_FILES);
                                for (IndexFile ix : indexList) {

                                    Element elIndexFile = addChildToDocument(doc, elIndexFiles, INDEX_FILE);
                                    //File details attribute
                                    updateXmlIndexFiles(doc, elIndexFile, INDEX_TYPE, ix.getIndexType(), INDEX_NAME, ix.getIndexName(), INDEX_IS_UNIQUE, Boolean.toString(ix.getIsUnique()));

                                    Element elIndexAttributes = addChildToDocument(doc, elIndexFile, ALL_INDEX_ATTRIBUTES);

                                    for (String a : ix.getAtttributes()) {
                                        addChildNodeToParent(doc, elIndexAttributes, a, INDEX_ATTRIBUTE);
                                    }
                                }
                            }
                            List<Attribute> attrList = aTableList.getAttributes();
                            for (Attribute anAttrList : attrList) {
                                Element elAttribute = doc.createElement(TABLE_ATTRIBUTE);
                                try {
                                    updateXmlAttributeProperties(doc, anAttrList, elAttribute);
                                    elStructure.appendChild(elAttribute);

                                } catch (Exception e) {
                                    System.out.println("Error create Attribute");
                                }
                            }
                        } catch (Exception e) {
                            System.out.println("can't create element");
                        }
                        elTables.appendChild(elTable);
                    }
                }
            }
            writeDocumentToFile(doc, DB_STRUCTURE_FILEPATH);
        } catch (ParserConfigurationException ex) {
            Logger.getLogger(DatabaseConnector.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Adds a simle child to a parent element
     *
     * @param document    document
     * @param parent      type of the new element
     * @param elementType parent element
     * @return added child
     */
    private static Element addChildToDocument(Document document, Element parent, String elementType) {
        Element child = document.createElement(elementType);
        parent.appendChild(child);
        return child;
    }

    /**
     * Updates the properties of an element
     *
     * @param document  document in which the property is added
     * @param element   element that needs an attribute
     * @param attribute name of the attribute
     * @param value     value of the attribute
     */
    private static void updateElementProperties(Document document, Element element, String attribute, String value) {
        Attr tableName = document.createAttribute(attribute);
        tableName.setValue(value);
        element.setAttributeNode(tableName);
    }

    /**
     * Adds the given information to the element
     */
    private static void updateXmlIndexFiles(Document document, Element element, String attr1, String val1, String attr2, String val2, String attr3, String val3) {
        updateElementProperties(document, element, attr1, val1);
        updateElementProperties(document, element, attr2, val2);
        updateElementProperties(document, element, attr3, val3);
    }

    /**
     * Copies the properties of an {@link Attribute} to an xml Element
     *
     * @param doc       document that will contain the element
     * @param attribute original attribute
     * @param element   element that will contains the information from the attribute
     */
    private static void updateXmlAttributeProperties(Document doc, Attribute attribute, Element element) {
        //Attr from elStructure -> attribute ----------------------------------------------------------------
        updateXmlIndexFiles(doc, element, ATTRIBUTE_NAME,
                attribute.getAttrName(), ATTRIBUTE_TYPE, attribute.getAttrType(),
                ATTRIBUTE_IS_NULL, Boolean.toString(attribute.isNull()));
    }

    /**
     * Adds a child to a given element
     *
     * @param doc    document that contains the information
     * @param parent parent node
     * @param value  value of the child
     * @param child  child node
     */
    private static void addChildNodeToParent(Document doc, Element parent, String value, String child) {
        Element elIAttr = doc.createElement(child);
        elIAttr.setTextContent(value);
        parent.appendChild(elIAttr);
    }

    /**
     * Writes a {@link Document} to a file
     *
     * @param document document to be written
     * @param filepath filepath where the document will be writtten
     */
    public static void writeDocumentToFile(Document document, String filepath) {
        try {
            TransformerFactory transformerFactory
                    = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(document);
            StreamResult result = new StreamResult(new File(filepath));
            transformer.transform(source, result);
        } catch (TransformerException e) {
            LOG.log(Level.SEVERE, e.getMessage());
        }
    }

    public static List<Database> readDatabasesFromXML() {
        List<Database> databases = new ArrayList<>();
        try {
            Document doc = loadXMLFromFile(DB_STRUCTURE_FILEPATH);

            NodeList nodesDbs = doc.getElementsByTagName(ALL_DATABASES);
            if (nodesDbs.getLength() > 0) {
                Element elDbs = (Element) nodesDbs.item(0);

                NodeList nodesDb = elDbs.getElementsByTagName(DATABASE);
                //System.out.println("Nodes length" + nodesDb.getLength());

                for (int i = 0; i < nodesDb.getLength(); i++) {
                    Element elDb = (Element) nodesDb.item(i);

                    NodeList nodesTbs = elDb.getElementsByTagName(ALL_TABLES);
                    if (nodesTbs.getLength() > 0) {
                        Element elTables = (Element) nodesTbs.item(0);

                        NodeList nodesTb = elTables.getElementsByTagName(TABLE);
                        List<Table> tableList = new ArrayList<>();
                        for (int j = 0; j < nodesTb.getLength(); j++) {
                            Element elTable = (Element) nodesTb.item(j);

                            //Pk attr value to list-----------------------------------------------------------------------
                            NodeList nodesPK = elTable.getElementsByTagName(PRIMARY_KEY);
                            List<String> pkAttrList = new ArrayList<>();
                            if (nodesPK.getLength() > 0) {
                                Element elPK = (Element) nodesPK.item(0);

                                NodeList nodesPKAttr = elPK.getElementsByTagName(PRIMARY_KEY_ATTRIBUTE);
                                for (int h = 0; h < nodesPKAttr.getLength(); h++) {
                                    Element elPKAttr = (Element) nodesPKAttr.item(h);
                                    pkAttrList.add(elPKAttr.getTextContent());
                                }
                            }

                            //unique key attr value list------------------------------------------------------------------
                            NodeList nodesUniqe = elTable.getElementsByTagName(UNIQUE_KEYS);
                            List<String> uniqueAttrList = new ArrayList<>();
                            if (nodesUniqe.getLength() > 0) {
                                Element elUnique = (Element) nodesUniqe.item(0);

                                NodeList nodesUniqueAttr = elUnique.getElementsByTagName(UNIQUE_ATTRIBUTE);
                                // nu stiu de ce da eroare de la 1 merge si de la 0 nu ca si la celelalte
                                for (int u = 0; u < nodesUniqueAttr.getLength(); u++) {
                                    Element elUniqueAttr = (Element) nodesUniqueAttr.item(u);
                                    uniqueAttrList.add(elUniqueAttr.getTextContent());
                                }
                            }
                            //foreign key value list--------------------------------------------------------------------------------------------------------------
                            NodeList nodesForeignKeys = elTable.getElementsByTagName(ALL_FOREIGN_KEYS);
                            List<ForeignKey> foreignKeyList = new ArrayList<>();
                            //System.out.println("ForeignKeys" + nodesForeignKeys.getLength());
                            if (nodesForeignKeys.getLength() > 0) {
                                Element elForeignKeys = (Element) nodesForeignKeys.item(0);

                                NodeList nodesFK = elForeignKeys.getElementsByTagName(FOREIGN_KEY);
                                for (int fk = 0; fk < nodesFK.getLength(); fk++) {
                                    Element elFK = (Element) nodesFK.item(fk);

                                    NodeList nodesFkAttr = elFK.getElementsByTagName(FOREIGN_KEY_ATTRIBUTE);
                                    Element elFkAttr = (Element) nodesFkAttr.item(0);
                                    // System.out.println("FkAttribute:"+elFkAttr.getTextContent());

                                    NodeList nodesRef = elFK.getElementsByTagName(TABLE_REFERENCES);
                                    Element elRef = (Element) nodesRef.item(0);
                                    // System.out.println("References");

                                    NodeList nodesRefTable = elRef.getElementsByTagName(REFERENCED_TABLE);
                                    Element elRefTable = (Element) nodesRefTable.item(0);

                                    NodeList nodesRefAttr = elRef.getElementsByTagName(REFERENCED_ATTRIBUTE);
                                    Element elRefAttr = (Element) nodesRefAttr.item(0);

                                    ForeignKey fkElem = new ForeignKey(elFkAttr.getTextContent(), elRefTable.getTextContent(), elRefAttr.getTextContent());
                                    foreignKeyList.add(fkElem);
                                }
                            }


                            NodeList nodesStructure = elTable.getElementsByTagName(TABLE_STRUCTURE);
                            List<Attribute> attrList = new ArrayList<>();
                            if (nodesStructure.getLength() > 0) {
                                Element elStructure = (Element) nodesStructure.item(0);

                                NodeList nodesAttr = elStructure.getElementsByTagName(TABLE_ATTRIBUTE);
                                for (int a = 0; a < nodesAttr.getLength(); a++) {
                                    Element elAttr = (Element) nodesAttr.item(a);

                                    String attrName = elAttr.getAttributes().getNamedItem(ATTRIBUTE_NAME).getNodeValue();
                                    String attrType = elAttr.getAttributes().getNamedItem(ATTRIBUTE_TYPE).getNodeValue();
                                    boolean isPrimaryKey = pkAttrList.contains(attrName);
                                    boolean isUnique = uniqueAttrList.contains(attrName);
                                    boolean isNull = Boolean.parseBoolean(elAttr.getAttributes().getNamedItem(ATTRIBUTE_IS_NULL).getNodeValue());
                                    String refTable = "";
                                    String refAttr = "";
                                    for (ForeignKey k : foreignKeyList) {
                                        if (k.getForeignKeyAttribute().equals(attrName)) {
                                            refTable = k.getReferencedTable();
                                            refAttr = k.getReferencedAttribute();
                                        }
                                    }
                                    Attribute attr = new Attribute(attrName, attrType, isPrimaryKey, isUnique, isNull, refTable, refAttr);
                                    attrList.add(attr);
                                }
                            }

                            //Index file node + index attr vale list--------------------------------------------------------
                            NodeList nodesIndexFiles = elTable.getElementsByTagName(ALL_INDEX_FILES);
                            List<IndexFile> indexFList = new ArrayList<>();
                            if (nodesIndexFiles.getLength() > 0) {
                                Element elInxesFiles = (Element) nodesIndexFiles.item(0);

                                NodeList nodesIndexF = elInxesFiles.getElementsByTagName(INDEX_FILE);
                                for (int f = 0; f < nodesIndexF.getLength(); f++) {
                                    Element elIndexF = (Element) nodesIndexF.item(f);

                                    //attributes with index
                                    NodeList nodesIAttributes = elIndexF.getElementsByTagName(ALL_INDEX_ATTRIBUTES);
                                    List<String> indexAttrList = new ArrayList<>();
                                    if (nodesIAttributes.getLength() > 0) {
                                        Element elIAttributes = (Element) nodesIAttributes.item(0);

                                        NodeList nodesIAttr = elIAttributes.getElementsByTagName(INDEX_ATTRIBUTE);

                                        for (int ix = 0; ix < nodesIAttr.getLength(); ix++) {
                                            Element elIAttr = (Element) nodesIAttr.item(ix);
                                            indexAttrList.add(elIAttr.getTextContent());
                                        }
                                    }
                                    // index file details
                                    String indexType = elIndexF.getAttributes().getNamedItem(INDEX_TYPE).getNodeValue();
                                    String indexName = elIndexF.getAttributes().getNamedItem(INDEX_NAME).getNodeValue();
                                    Boolean isUnique = Boolean.parseBoolean(elIndexF.getAttributes().getNamedItem(INDEX_IS_UNIQUE).getNodeValue());

                                    IndexFile indexFile = new IndexFile(indexType, isUnique, indexName, indexAttrList);
                                    indexFList.add(indexFile);
                                }
                            }

                            // create elTable attr + pk+ unique + indexfiles
                            Table tb = new Table(elTable.getAttributes().getNamedItem(TABLE_NAME).getNodeValue(), attrList, indexFList);
                            tableList.add(tb);

                        }
                        Database db = new Database(elDb.getAttributes().getNamedItem(DATABASE_NAME).getNodeValue(), tableList);
                        databases.add(db);
                    }
                }
            }
            return databases;
        } catch (NullPointerException ex) {
            Logger.getLogger(DatabaseConnector.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static boolean deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (null != files) {
                for (int i = 0; i < files.length; i++) {
                    if (files[i].isDirectory()) {
                        deleteDirectory(files[i]);
                    } else {
                        files[i].delete();
                    }
                }
            }
        }
        return (directory.delete());
    }
}
