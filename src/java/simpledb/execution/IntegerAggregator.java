package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupFieldIndex;
    private Type groupFieldType;
    private int aggregateFieldIndex;
    private Op aggregateOperator;
    private TupleDesc aggregateTd;
    private Map<Field, AggInfo> mapAggInfo;

    private class AggInfo {
        public AggInfo(int c, int s, Tuple t) {
            this.count = c;
            this.sum = s;
            this.tuple = t;
        }
        public int count;
        public int sum;
        public Tuple tuple;
    }
    /**
     * Aggregate constructor
     * 
     * @param gbfield
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
        if (gbfieldtype != null) {
            groupFieldIndex = gbfield;
            groupFieldType = gbfieldtype;
            Type[] types = new Type[2];
            types[0] = groupFieldType;
            types[1] = Type.INT_TYPE;
            aggregateTd = new TupleDesc(types);
        } else {
            groupFieldIndex = -1;
            groupFieldType = null;
            Type[] types = new Type[1];
            types[0] = Type.INT_TYPE;
            aggregateTd = new TupleDesc(types);
        }
        aggregateFieldIndex = afield;
        aggregateOperator = what;
        mapAggInfo = new HashMap<>();
    }

    private void mergeFieldWithOp(AggInfo oldAgg, IntField newF) {
        int newVal = 0;
        int index;
        IntField oldF;
        if (this.groupFieldType != null) {
            index = 1;
        } else {
            index = 0;
        }
        // oldF may be null.
        oldF = (IntField)oldAgg.tuple.getField(index);
        switch (this.aggregateOperator) {
            case AVG:
                newVal = (oldAgg.sum + newF.getValue())/(oldAgg.count+1);
                break;
            case MIN:
                if (oldF != null) {
                    newVal = oldF.getValue() < newF.getValue() ? oldF.getValue() : newF.getValue();
                } else {
                    newVal = newF.getValue();
                }
                break;
            case MAX:
                if (oldF != null) {
                    newVal = oldF.getValue() > newF.getValue() ? oldF.getValue() : newF.getValue();
                } else {
                    newVal = newF.getValue();
                }
                break;
            case SUM:
                if (oldF != null) {
                    newVal = oldF.getValue() + newF.getValue();
                } else {
                    newVal = newF.getValue();
                }
                break;
            case COUNT:
                if (oldF != null) {
                    newVal = oldF.getValue() + 1;
                } else {
                    newVal = 1;
                }
                break;
            default:
                throw new RuntimeException("not support in mergeFieldWithOp");
        }
        oldAgg.count ++;
        oldAgg.sum += newF.getValue();
        IntField result = new IntField(newVal);
        oldAgg.tuple.setField(index, result);
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
        Field gtf;
        if (this.groupFieldType != null) {
            gtf = tup.getField(this.groupFieldIndex);
        } else {
            gtf = new IntField(-1);
        }

        Field atf = tup.getField(this.aggregateFieldIndex);
        AggInfo aggInfo = this.mapAggInfo.get(gtf);
        if (aggInfo == null) {
            Tuple aggTuple =  new Tuple(this.aggregateTd);
            if (this.groupFieldType != null) {
                aggTuple.setField(0, gtf);
            }
            aggInfo = new AggInfo(0, 0, aggTuple);
            this.mapAggInfo.put(gtf, aggInfo);
        }
        mergeFieldWithOp(aggInfo, (IntField) atf);
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
        // some code goes here
        List<Tuple> listTuple = new ArrayList<>();
        for (AggInfo aggInfo : this.mapAggInfo.values()) {
            listTuple.add(aggInfo.tuple);
        }
        return new TupleIterator(this.aggregateTd, listTuple);
    }

}
