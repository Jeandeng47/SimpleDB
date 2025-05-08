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
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

   private int gbfield;
   private Type gbfieldtype;
   private int afield;
   private Op aggrOp;
   private HashMap<Field, Integer> aggregates;
   private HashMap<Field, Integer> counts; // for computing AVG

    /**
     * Aggregate constructor
     * 
     * @param gbfieldb
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.aggrOp = what;
        this.aggregates = new HashMap<>();
        this.counts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here  
        Field groupKey = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        IntField agField = (IntField) tup.getField(afield); 
        int val = agField.getValue();

        // Get old group values
        int count = counts.getOrDefault(groupKey, 0);
        int agg = aggregates.getOrDefault(groupKey, initAggregates());

        // Update aggregates based on the operation
        switch (this.aggrOp) {
            case COUNT:
                agg = count + 1;
                break;
            case SUM:
                agg += val;
                break;
            case MIN:
                agg = Math.min(agg, val);
                break;
            case MAX:
                agg = Math.max(agg, val);
                break;
            case AVG:
                agg += val; // update SUM part, update COUNT in counts
                break;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation");
        }

        // Update counts and aggregates
        counts.put(groupKey, count + 1);
        aggregates.put(groupKey, agg);
    }
        
    // Helper method to initialize aggregates based on the operation
    private int initAggregates() {
        switch (this.aggrOp) {
            case COUNT: return 0;
            case SUM: return 0;
            case AVG: return 0;
            case MIN: return Integer.MAX_VALUE;
            case MAX: return Integer.MIN_VALUE;
            default: throw new UnsupportedOperationException("Unsupported aggregation");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        List<Tuple> aggrResults = new ArrayList<>();
        TupleDesc td;
        // some code goes here
        if (this.gbfield != Aggregator.NO_GROUPING) {
            // Group by case, tuple in form (groupVal, aggregateVal)
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            for (Field groupKey: aggregates.keySet()) {
                Tuple t = new Tuple(td);
                int groupVal = getAggregate(groupKey);
                t.setField(0, groupKey);
                t.setField(1, new IntField(groupVal));
                aggrResults.add(t);
            }
        } else {
            // No grouping, tuple in form (aggregateVal)
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple t = new Tuple(td);
            int groupVal = getAggregate(null);
            t.setField(0, new IntField(groupVal));
            aggrResults.add(t);

        }
        return new TupleIterator(td, aggrResults);
    }

    // Helper method to get the aggregate value
    private int getAggregate(Field groupKey) {
        if (this.aggrOp == Op.AVG) {
            int count = counts.get(groupKey);
            int sum = aggregates.get(groupKey);
            return sum / count;
        } else {
            return aggregates.get(groupKey);
        }
    }

}

