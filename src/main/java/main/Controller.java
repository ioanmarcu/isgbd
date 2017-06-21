package main;

import entity.Attribute;
import entity.Database;
import entity.SelectItem;
import entity.Table;
import repository.RepoBerekeley;
import repository.RepoXML;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author I. Marcu
 */
public class Controller {

    RepoBerekeley repoB = new RepoBerekeley();
    RepoXML repoXML = new RepoXML(repoB);

    public Controller() {

    }
//Berkley Functions ------------------------------------------------------------

    //INSERT entry into Table-------------------------------------------------------------
    public void insertEntry(List<String> listAttrValue, String dbName, String tbName) throws Exception {
        repoB.insertEntry(listAttrValue, dbName, tbName);
    }

    // LOAD FROM TABLE--------------------------------------------------------------
// DELETE entry with specific Key-----------------------------------------------
    public void deleteEntry(String keyEntryValue, String dbName, String tbName) throws Exception {
        repoB.deleteEntry(keyEntryValue, dbName, tbName);
    }

    public void createIndexForAttribute(String attrIndexStructure, String dbName, String tbName) throws Exception {
        repoB.createIndexForAttribute(attrIndexStructure, dbName, tbName);
    }

    public void createDatabase(String dbName) throws Exception{
        repoXML.createDatabase(dbName);
    }

    public void createTable(String dbName, String tbName) throws Exception {
        repoXML.createTable(dbName, tbName);
    }

    public void deleteDbFromDatabases(String dbName) throws Exception {
        repoXML.deleteDbFromDatabases(dbName);
    }

    public void addTableToDatabase(String dbName, Table tb) throws Exception {
        repoXML.addTableToDatabase(dbName, tb);
    }

    public void deleteTableFromDatabase(String dbName, String tbName)  throws Exception{

        repoXML.deleteTableFromDatabase(dbName, tbName);
    }

    public void addAttributeToTable(String dbName, String tbName, String attrName, String attrType, boolean isPrimaryKey, boolean isUnique, boolean isNull, String refTable, String refAttr) throws Exception {
        repoXML.addAttributeToTable(dbName, tbName, attrName, attrType, isPrimaryKey, isUnique, isNull, refTable, refAttr);
    }

    public void addIndexToTable(String attrName, String dbName, String tbName) throws Exception {
        //TRebuie modificat ceva la ai cu XML ca da eroare dupa ce scrie o data
        //repoXML.addIndexToTable(attrName, dbName, tbName);
        repoB.createIndexForAttribute(attrName, dbName, tbName);
    }

    public void deleteAtributeFromTable(String dbName, String tbName, String attrName) throws Exception {
        repoXML.deleteAtributeFromTable(dbName, tbName, attrName);
    }

    public List<Database> getDatabases() {
        return repoXML.getDatabases();
    }

    public List<Table> getTablesOfDb(String dbName) {
        return repoXML.getTablesOfDb(dbName);
    }

    public List<Attribute> getAttributesOfTableOfDb(String dbName, String tbName) {
        return repoXML.getAttributesOfTableOfDb(dbName, tbName);
    }

    public List<Attribute> getPrimayKeyAttributeOfTableOfDb(String dbName, String tbName) {
        return repoXML.getPrimayKeyAttributeOfTableOfDb(dbName, tbName);
    }

    public String toStringDatabase() {
        return repoXML.toStringDatabase();
    }

    //LOAD for the moment only prints
    public String[][] loadDataFromTable(String dbName, String tbName) throws Exception {
        return repoB.loadDataFromTable(dbName, tbName);
    }

    public List<String> loadDataFromTableInList(String dbName, String tbName) throws Exception {
        List<String> result = new ArrayList<String>();
        result = repoB.loadDataFromTableInList(dbName, tbName);
        return result;
    }

    public void loadDataFromIndex(String attrIndexStructure, String dbName, String tbName) throws Exception {
        repoB.loadDataFromIndex(attrIndexStructure, dbName, tbName);
    }

    public List<String> loadDataFromIndexInList(String attrIndexStructure, String dbName, String tbName) throws Exception {
        List<String> result = new ArrayList<String>();
        result = repoB.loadDataFromIndexInList(attrIndexStructure, dbName, tbName);
        return result;
    }

    public List<String> select(List<SelectItem> forSelect) throws Exception {
        List<String> result = new ArrayList<String>();
        result = repoB.select(forSelect);
        return result;
    }

    public List<String> lessThan(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> result = new ArrayList<String>();
        result = repoB.lessThan(column, tbName, dbName, filter);
        return result;
    }

    public List<String> greaterThan(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> result = new ArrayList<String>();
        result = repoB.greaterThan(column, tbName, dbName, filter);
        return result;
    }

    public List<String> equalWith(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> result = new ArrayList<String>();
        result = repoB.equalWith(column, tbName, dbName, filter);
        return result;
    }

    public List<String> lessThanOrEqualWith(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> result = new ArrayList<String>();
        result = repoB.lessThanOrEqualWith(column, tbName, dbName, filter);
        return result;
    }

    public List<String> greaterThanOrEqualWith(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> result = new ArrayList<String>();
        result = repoB.greaterThanOrEqualWith(column, tbName, dbName, filter);
        return result;
    }

    public List<String> between(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> result = new ArrayList<String>();
        result = repoB.between(column, tbName, dbName, filter);
        return result;
    }

    public List<String> differentFrom(String column, String tbName, String dbName, String filter) throws Exception {
        List<String> result = new ArrayList<String>();
        result = repoB.differentFrom(column, tbName, dbName, filter);
        return result;
    }

    public List<String> sortAscending(List<String> filtered) {
        repoB.sortAscending(filtered);
        return filtered;
    }

    public List<String> sortDescending(List<String> filtered) {
        repoB.sortDescending(filtered);
        return filtered;
    }

    public List<String> sort(List<String> list, String attribute, String order,String dbName, String tbName){
        List<String> result = new ArrayList<String>();
        result = repoB.sort(list, attribute, order, dbName, tbName);
        return result;
    }

    public List<String> sum(List<String> list,String attribute, String alias, String groupBy,
                            String dbName, String tbName) throws Exception{
        List<String> result = new ArrayList<String>();
        result = repoB.sum(list,attribute, alias, groupBy, dbName, tbName);
        return result;
    }

    public List<String> count(List<String> list,String attribute, String alias, String groupBy,
                              String dbName, String tbName) throws Exception{
        List<String> result = new ArrayList<String>();
        result = repoB.count(list,attribute, alias, groupBy, dbName, tbName);
        return result;
    }

    public List<String> avg(List<String> list,String attribute, String alias, String groupBy,
                            String dbName, String tbName) throws Exception{
        List<String> result = new ArrayList<String>();
        result = repoB.avg(list,attribute, alias, groupBy, dbName, tbName);
        return result;
    }

    public List<String> min(List<String> data, String attribute, String dbName, String tbName) throws Exception{
        List<String> result = new ArrayList<String>();
        result = repoB.min(data, attribute, dbName, tbName);
        return result;
    }

    public List<String> max(List<String> data, String attribute, String dbName, String tbName) throws Exception{
        List<String> result = new ArrayList<String>();
        result = repoB.max(data, attribute, dbName, tbName);
        return result;
    }

    public List<String> having(List<String> list, String attribute,String cond,String dbName,
                               String tbName, boolean minMax){
        List<String> result = new ArrayList<String>();
        result = repoB.having(list, attribute, cond, dbName, tbName,minMax);
        return result;
    }

    public List<String> union(List<String> listA, List<String>listB){
        List<String> list = new ArrayList<String>();
        list = repoB.union(listA, listB);
        return list;
    }

    public List<String> intersect(List<String> listA, List<String>listB){
        List<String> list = new ArrayList<String>();
        list = repoB.intersect(listA, listB);
        return list;
    }

    public List<String> except(List<String> listA, List<String>listB){
        List<String> list = new ArrayList<String>();
        list = repoB.except(listA, listB);
        return list;
    }

    public String innerJoinIndexNestedLoop(String dbName, String tbName1, String tbName2, String attr1, String attr2) throws Exception {
        return  repoB.innerJoinIndexNestedLoop(dbName, tbName1, tbName2, attr1, attr2);
    }

    public String innerJoinSortMerge(String dbName, String tbName1, String tbName2, String attr1, String attr2) throws Exception {
        return  repoB.innerJoinSortMerge(dbName, tbName1, tbName2, attr1, attr2);
    }
}
