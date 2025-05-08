package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc; // schema of this tuple
    private Field[] fields; // array of fields
    private RecordId recordId; // location of this tuple on disk

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        if (td == null || td.numFields() == 0) {
            throw new IllegalArgumentException("TupleDesc cannot be null or empty");
        }
        this.tupleDesc = td;
        this.fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i < 0 || i >= fields.length) {
            throw new IndexOutOfBoundsException("Field index out of bounds");
        }
        if (f.getType() != tupleDesc.getFieldType(i)) {
            throw new IllegalArgumentException("Field type mismatch");
        }
        this.fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i < 0 || i >= fields.length) {
            throw new IndexOutOfBoundsException("Field index out of bounds");
        }
        return this.fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            sb.append(fields[i].toString());
            if (i == fields.length - 1) {
                sb.append("\n");
            } else {
                sb.append("\t");
            }
        }
        return sb.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new FieldIterator();
        
    }

    private class FieldIterator implements Iterator<Field> {
        private int index = 0;

        public boolean hasNext() {
            return index < fields.length;
        }

        public Field next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return fields[index++];
        }
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        if (td == null || td.numFields() == 0) {
            throw new IllegalArgumentException("TupleDesc cannot be null or empty");
        }
        this.tupleDesc = td;
    }

    public static Tuple mergeTuples(Tuple t1, Tuple t2) {
        TupleDesc td1 = t1.getTupleDesc();
        TupleDesc td2 = t2.getTupleDesc();
        TupleDesc merged = TupleDesc.merge(td1, td2);

        Tuple mergedTuple = new Tuple(merged);
        for (int i = 0; i < td1.numFields(); i++) {
            mergedTuple.setField(i, t1.getField(i));
        }
        for (int i = 0; i < td2.numFields(); i++) {
            mergedTuple.setField(i + td1.numFields(), t2.getField(i));
        }
        return mergedTuple;
    }
}
