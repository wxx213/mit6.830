package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator tableOpIterator;
    private int aggregateFieldIndex;
    private int groupFieldIndex;
    private Type groupFieldType;
    private Aggregator.Op aggregateOp;
    private TupleDesc aggregateTd;
    private Aggregator aggregator;
    private OpIterator aggregatorOpIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        tableOpIterator = child;
        aggregateFieldIndex = afield;
        groupFieldIndex = gfield;
        aggregateOp = aop;
        if (groupFieldIndex != -1) {
            groupFieldType = tableOpIterator.getTupleDesc().getFieldType(groupFieldIndex);
        } else {
            groupFieldType = null;
        }

        Type type = tableOpIterator.getTupleDesc().getFieldType(aggregateFieldIndex);
        if (type == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(groupFieldIndex, groupFieldType, aggregateFieldIndex, aggregateOp);
        } else {
            aggregator = new StringAggregator(groupFieldIndex, groupFieldType, aggregateFieldIndex, aggregateOp);
        }
        try {
            tableOpIterator.open();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        while (true) {
            try {
                if (!tableOpIterator.hasNext()) break;
            } catch (DbException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            }
            try {
                Tuple tuple = tableOpIterator.next();
                aggregator.mergeTupleIntoGroup(tuple);
            } catch (DbException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            }

        }
        tableOpIterator.close();
        aggregatorOpIterator = aggregator.iterator();
        aggregateTd = aggregatorOpIterator.getTupleDesc();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.groupFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return -1;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return null;
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return null;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.aggregatorOpIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.aggregatorOpIterator.hasNext()) {
            return this.aggregatorOpIterator.next();
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.aggregatorOpIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return aggregateTd;
    }

    public void close() {
        // some code goes here
        this.aggregatorOpIterator.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}
