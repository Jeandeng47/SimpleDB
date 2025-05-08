package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op aggrOp;
    private HashMap<Field, Integer> counts;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Only COUNT is supported for StringAggregator");
        }
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.aggrOp = what;
        this.counts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupKey = (gbfield == Aggregator.NO_GROUPING) ? null : tup.getField(gbfield);
        int count = counts.getOrDefault(groupKey, 0);
        counts.put(groupKey, count + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> aggrResults = new ArrayList<>();
        TupleDesc td;
        if (gbfield != Aggregator.NO_GROUPING) {
            // Group by case, tuple in form (groupVal, aggregateVal)
            td = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE});
            for (Field groupKey : counts.keySet()) {
                Tuple t = new Tuple(td);
                t.setField(0, groupKey);
                t.setField(1, new IntField(counts.get(groupKey)));
                aggrResults.add(t);
            }
        } else {
            // no grouping, tuple in form (aggregateVal)
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(counts.get(null)));
            aggrResults.add(t);
        }
        return new TupleIterator(td, aggrResults);
    }

}
