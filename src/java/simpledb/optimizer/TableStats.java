package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableId;
    private int ioCostPerPage;
    private Map<Integer, ColHistogramInfo> mapColumnHistogram;
    private int numTuples;

    private class ColHistogramInfo {
        public Type fieldType;
        public IntHistogram intHistogram;
        public StringHistogram stringHistogram;
    }

    private Map<Integer, ArrayList> fetchFieldValues(int tableId) {
        Map<Integer, ArrayList> result = new HashMap<>();
        SeqScan seqScan = new SeqScan(new TransactionId(), tableId);
        for (int i=0;i<seqScan.getTupleDesc().numFields();i++) {
            Type type = seqScan.getTupleDesc().getFieldType(i);
            if (type == Type.INT_TYPE) {
                result.put(i, new ArrayList<Integer>());
            } else {
                result.put(i, new ArrayList<String>());
            }
        }

        try {
            seqScan.open();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                if (!seqScan.hasNext()) break;
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            } catch (DbException e) {
                e.printStackTrace();
            }
            try {
                Tuple tuple = seqScan.next();
                for (int i=0;i<tuple.getTupleDesc().numFields();i++) {
                    ArrayList list = result.get(i);
                    Type type = tuple.getField(i).getType();
                    if (type == Type.INT_TYPE) {
                        IntField intField = (IntField)tuple.getField(i);
                        list.add(intField.getValue());
                    } else {
                        StringField stringField = (StringField) tuple.getField(i);
                        list.add(stringField.getValue());
                    }
                }
                this.numTuples ++;
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            } catch (DbException e) {
                e.printStackTrace();
            }
        }
        seqScan.close();
        return result;
    }

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.mapColumnHistogram = new HashMap<>();
        this.numTuples = 0;

        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(this.tableId);
        TupleDesc tupleDesc = heapFile.getTupleDesc();

        Map<Integer, ArrayList> fieldValues = fetchFieldValues(this.tableId);
        for (int key : fieldValues.keySet()) {
            ColHistogramInfo colHistogramInfo = this.mapColumnHistogram.get(key);
            if (colHistogramInfo == null) {
                colHistogramInfo = new ColHistogramInfo();
                this.mapColumnHistogram.put(key, colHistogramInfo);
            }
            if (tupleDesc.getFieldType(key) == Type.INT_TYPE) {
                ArrayList<Integer> arrayList = fieldValues.get(key);
                if (colHistogramInfo.intHistogram == null) {
                    IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS,
                            Collections.min(arrayList),
                            Collections.max(arrayList));
                    colHistogramInfo.fieldType = Type.INT_TYPE;
                    colHistogramInfo.intHistogram = intHistogram;
                }
                for (int value : arrayList) {
                    colHistogramInfo.intHistogram.addValue(value);
                }
            } else {
                ArrayList<String> arrayList = fieldValues.get(key);
                if (colHistogramInfo.stringHistogram == null) {
                    StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                    colHistogramInfo.fieldType = Type.STRING_TYPE;
                    colHistogramInfo.stringHistogram = stringHistogram;
                }
                for (String value : arrayList) {
                    colHistogramInfo.stringHistogram.addValue(value);
                }
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(this.tableId);
        double result = heapFile.numPages() * this.ioCostPerPage;
        return result;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (this.numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(this.tableId);
        TupleDesc tupleDesc = heapFile.getTupleDesc();
        ColHistogramInfo colHistogramInfo = this.mapColumnHistogram.get(field);
        if (tupleDesc.getFieldType(field) == Type.INT_TYPE) {
            IntField intField = (IntField) constant;
            return colHistogramInfo.intHistogram.estimateSelectivity(op, intField.getValue());
        } else {
            StringField stringField = (StringField) constant;
            return colHistogramInfo.stringHistogram.estimateSelectivity(op, stringField.getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return 0;
    }

}
