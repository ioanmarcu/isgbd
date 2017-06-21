package entity;

/**
 * @author I. Marcu
 */
public class SelectItem {

    private String attr;
    private String dbName;
    private String tbName;
    private boolean distinct;
    private boolean output;
    private String sortType;
    private String filter;
    private boolean groupBy;
    private String aggFunction;
    private String alias;
    private String attrGroupBy;
    private boolean having;
    private String condition;

    public SelectItem() {
    }

    public SelectItem(String attr, String dbName, String tbName, boolean distinct, boolean output,
                      String sortType, String filter, boolean groupBy, String aggFunction, String alias, String attrGroupBy,
                      boolean having, String condition) {
        this.attr = attr;
        this.tbName = tbName;
        this.dbName = dbName;
        this.distinct = distinct;
        this.output = output;
        this.sortType = sortType;
        this.filter = filter;
        this.groupBy = groupBy;
        this.aggFunction = aggFunction;
        this.alias = alias;
        this.attrGroupBy = attrGroupBy;
        this.having = having;
        this.condition = condition;
    }

    public String getAttr() {
        return attr;
    }

    public void setAttr(String attr) {
        this.attr = attr;
    }

    public String getTbName() {
        return tbName;
    }

    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public boolean isOutput() {
        return output;
    }

    public void setOutput(boolean output) {
        this.output = output;
    }

    public String getSortType() {
        return sortType;
    }

    public void setSortType(String sortType) {
        this.sortType = sortType;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public boolean isGroupBy() {
        return groupBy;
    }

    public void setGroupBy(boolean groupBy) {
        this.groupBy = groupBy;
    }

    public String getAggFunction() {
        return aggFunction;
    }

    public void setAggFunction(String aggFunction) {
        this.aggFunction = aggFunction;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getAttrGroupBy() {
        return attrGroupBy;
    }

    public void setAttrGroupBy(String attrGroupBy) {
        this.attrGroupBy = attrGroupBy;
    }

    public boolean isHaving() {
        return having;
    }

    public void setHaving(boolean having) {
        this.having = having;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }


}
