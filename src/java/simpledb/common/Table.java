package simpledb.common;

import simpledb.storage.DbFile;
import simpledb.storage.TupleDesc;

public class Table {

    private final DbFile dbFile;
    private final String tableName;
    private final String primaryKey;
    private final TupleDesc tupleDesc;

    public Table(DbFile dbFile, String tableName, String primaryKey) {
        this.dbFile = dbFile;
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.tupleDesc = dbFile.getTupleDesc();
    }

    public DbFile getDbFile() {
        return dbFile;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }
}
