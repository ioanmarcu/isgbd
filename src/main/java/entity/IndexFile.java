package entity;

import java.util.ArrayList;
import java.util.List;

/**
 * @author I. Marcu
 */
public class IndexFile {

    private String indexType;
    private Boolean isUnique;
    private String indexName;
    private List<String> atttributes;

    public IndexFile(String indexType, Boolean isUnique, String indexName, List<String> atttributes) {
        this.indexType = indexType;
        this.isUnique = isUnique;
        this.indexName = indexName;
        this.atttributes = atttributes != null ? atttributes : new ArrayList<>();
    }

    public IndexFile(String indexName) {

        this.indexName = indexName;
    }
    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public Boolean getIsUnique() {
        return isUnique;
    }

    public void setIsUnique(Boolean isUnique) {
        this.isUnique = isUnique;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<String> getAtttributes() {
        return atttributes;
    }

    public void setAtttributes(List<String> atttributes) {
        this.atttributes = atttributes;
    }

    public void addAttrNameToIndex(String arrtName) {
        atttributes.add(arrtName);
    }

    public void deleteAttrNmeFromIndex(String attrName) {
        atttributes.remove(attrName);
    }

    @Override
    public String toString() {
        return "IndexFile{" + "indexType=" + indexType + ", isUnique=" + isUnique + ", indexName=" + indexName + ", atttributes=" + atttributes + '}';
    }

}
