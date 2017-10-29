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

    public static String createDatabase(String databaseName) throws Exception {
        return "Successfully created database";
    }

    public static String dropDatabase(String databaseName) throws Exception {
        return null;
    }

    public static List<Database> readDatabasesFromXML() {
        return null;
    }
}
