package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private TDItem[] tdItems;
    private int numFields;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new TDIterator();
    }

    private class TDIterator implements Iterator<TDItem> {
        private int index = 0;

        public boolean hasNext() {
            return index < numFields;
        }

        public TDItem next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return tdItems[index++];
        }
    }
           

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("Type array must contain at least one entry");
        }

        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("Type and field arrays must be of the same length");
        }

        this.numFields = typeAr.length;
        this.tdItems = new TDItem[this.numFields];

        for (int i = 0; i < numFields; i++) {
            this.tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
        }

    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    // Construct TupleDesc with TDItems array
    public TupleDesc(TDItem[] tdItems) {
        // some code goes here
        if (tdItems.length == 0) {
            throw new IllegalArgumentException("TDItems array must contain at least one entry");
        }
        this.tdItems = tdItems;
        this.numFields = tdItems.length;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields) {
            throw new NoSuchElementException("Index out of bounds");
        }
        return tdItems[i].fieldName; // could be null
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields) {
            throw new NoSuchElementException("Index out of bounds");
        }
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException("Field name cannot be null");
        }
        for (int i = 0; i < numFields; i++) {
            if (name.equals(tdItems[i].fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException("Field name not found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int totalSize = 0;
        for (TDItem item : tdItems) {
            totalSize += item.fieldType.getLen();
        }
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int totalFields = td1.numFields() + td2.numFields();
        TDItem[] mergedItems = new TDItem[totalFields];

        for (int i = 0; i < td1.numFields(); i++) {
            mergedItems[i] = td1.tdItems[i];
        }

        for (int i = 0; i < td2.numFields(); i++) {
            mergedItems[td1.numFields() + i] = td2.tdItems[i];
        }

        return new TupleDesc(mergedItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleDesc that = (TupleDesc) o;

        if (this.numFields != that.numFields) return false;
        for (int i = 0; i < this.numFields; i++) {
            if (!this.tdItems[i].fieldType.equals(that.tdItems[i].fieldType)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        Type[] types = new Type[numFields];
        for (int i = 0; i < numFields; i++) {
            types[i] = tdItems[i].fieldType;
        }
        return Arrays.hashCode(types);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numFields; i++) {
            sb.append(tdItems[i].toString());
            if (i != numFields - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
