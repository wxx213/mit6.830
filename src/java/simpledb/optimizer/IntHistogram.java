package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int bucketNum;
    private int minInput;
    private int maxInput;
    private int plusNum;
    private int interval;
    private int inputCount;

    Map<Integer, BucketValue> bucketKeyMap;

    private class BucketValue {
        private int min;
        private int max;
        private int count;

        public BucketValue(int min, int max, int count) {
            this.min = min;
            this.max = max;
            this.count = count;
        }
    }

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        bucketNum = buckets;
        minInput = min;
        maxInput = max;
        interval = (max - min + 1) / buckets;
        plusNum = (max - min + 1) % buckets;
        bucketKeyMap = new HashMap<>();
        inputCount = 0;
        int curentVal = min;
        BucketValue bucketValue;
        for (int i=0;i<buckets;i++) {
            if (i<plusNum) {
                bucketValue = new BucketValue(curentVal, curentVal+this.interval, 0);
                curentVal = curentVal + this.interval + 1;
            } else {
                bucketValue = new BucketValue(curentVal, curentVal+this.interval-1, 0);
                curentVal = curentVal + this.interval;
            }
            bucketKeyMap.put(i, bucketValue);
        }
    }

    private int getMapBucketKey(int value) {
        if (value < this.minInput) {
            return -1;
        } else if (value > this.maxInput) {
            return this.bucketNum;
        }
        int middle = minInput + (interval + 1) * plusNum;
        if (value < middle) {
            return (value - minInput) / (interval + 1);
        } else {
            return plusNum + (value - middle) / (interval);
        }
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = getMapBucketKey(v);
        if (index < this.bucketNum && index >= 0) {
            BucketValue value = bucketKeyMap.get(index);
            value.count++;
            bucketKeyMap.put(index, value);
            inputCount ++;
        }
    }

    private int doEquals(int index) {
        int result = 0;
        if (index < this.bucketNum && index >= 0) {
            result = bucketKeyMap.get(index).count;
        }
        return result;
    }

    private int doGreaterThan(int index) {
        int result = 0;
        for (int i=index + 1;i<bucketNum;i++) {
            result = bucketKeyMap.get(i).count + result;
        }
        return result;
    }

    private int doLessThan(int index) {
        int result = 0;
        for (int i=0;i<index;i++) {
            result = bucketKeyMap.get(i).count + result;
        }
        return result;
    }
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        int index = getMapBucketKey(v);
        int result = 0;
        switch (op) {
            case EQUALS:
                result = doEquals(index);
                break;
            case GREATER_THAN:
                result = doGreaterThan(index);
                break;
            case LESS_THAN:
                result = doLessThan(index);
                break;
            case GREATER_THAN_OR_EQ:
                result = doGreaterThan(index) + doEquals(index);
                break;
            case LESS_THAN_OR_EQ:
                result = doLessThan(index) + doEquals(index);
                break;
            case NOT_EQUALS:
                result = this.inputCount - doEquals(index);
                break;
            default:
        }
        return  (double) result / (double) inputCount;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
