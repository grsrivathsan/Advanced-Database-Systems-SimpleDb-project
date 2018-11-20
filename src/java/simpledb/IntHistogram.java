package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

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
	private int[] bucketArray;
	private int min,numBuckets;
	private int totEntries = 0;
	private int avgBucketSize;
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.numBuckets = buckets;
    	this.min = min;
    	//this.max = max;
    	this.bucketArray = new int[buckets];
    	this.totEntries = 0;
    	this.avgBucketSize = (int) Math.ceil((double)(max - min + 1) / buckets);
    	
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	
    	 int bucket = (v - this.min)/this.avgBucketSize;
         this.bucketArray[bucket]++;
         this.totEntries = this.totEntries + 1;
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

    	int bucketIndex = (v - this.min)/this.avgBucketSize;
        if (bucketIndex < 0)
        	bucketIndex = -1;
        if (bucketIndex >= this.numBuckets)
        	bucketIndex = this.numBuckets;
        
    	
    	
    	double estimateEquals = 0.0; 	
    	int flag = 0;
    	if(bucketIndex < 0 || (bucketIndex >= this.numBuckets))
    		flag = 1;    	
    	
    	if(flag == 0)
    	{
    		int h = this.bucketArray[bucketIndex];
    		estimateEquals = (double) ((double)h / avgBucketSize) / totEntries;
    	}
    		
    	
    	// some code goes here
    	
    	int fbucket,rbucket,lbucket,h;
    	if(bucketIndex < 0)
    	{
    		rbucket = fbucket = h = 0;
    		lbucket = -1;
    	}
    	else if(numBuckets <= bucketIndex)
    	{
    		rbucket = numBuckets;
    		lbucket = numBuckets - 1;
    		fbucket = h = 0;
    	}
    	else
    	{
    		rbucket = bucketIndex  + 1;
    		lbucket = bucketIndex - 1;
    		fbucket = -1;
    		h = bucketArray[bucketIndex];
       	}
    	
    	double estimateInEquals = 0.0;
    	if((op == Predicate.Op.GREATER_THAN) || (op == Predicate.Op.GREATER_THAN_OR_EQ))
    	{
    		if(fbucket == -1)
    		{
    			fbucket = ((rbucket * this.avgBucketSize) + this.min - v ) / avgBucketSize;
    		}
    		
    		estimateInEquals = (h * fbucket) / totEntries;
    		int flag1 = 0;
    		if(rbucket >= numBuckets)
    		{
    			estimateInEquals = estimateInEquals / totEntries;
    			flag1 = 1;
    		}
    		if(flag1 == 0)
    		{
    			for(int i = rbucket; i < numBuckets;i++)
    				estimateInEquals += this.bucketArray[i];
    			
    			estimateInEquals = estimateInEquals / totEntries;
    		}
    	}
    	
    	else if((op == Predicate.Op.LESS_THAN) || (op == Predicate.Op.LESS_THAN_OR_EQ))
    	{
    		if(fbucket == -1)
    			fbucket = (v - (lbucket * this.avgBucketSize) + min) / this.avgBucketSize ;
    		estimateInEquals = (h * fbucket) / totEntries ;
    		int flag2 = 0;
    		if(lbucket < 0)
    		{
    			estimateInEquals = estimateInEquals / totEntries ;
    			flag2 = 1;
    		}
    		if(flag2 == 0)
    		{
    			for(int i = lbucket;i >= 0;i--)
    			{
    				estimateInEquals += this.bucketArray[i];
    			}
    			estimateInEquals = estimateInEquals / this.totEntries;
    		}
    		
    	}
    	else
    	{
    		estimateInEquals = -1.0;
    	}
    	
    	switch(op)
    	{
    	case EQUALS:
    	case LIKE:
    		    return estimateEquals;
    	case GREATER_THAN:
    	case LESS_THAN:
    		return estimateInEquals;
    	case LESS_THAN_OR_EQ:
    		return estimateEquals + estimateInEquals;
    	case GREATER_THAN_OR_EQ:
    		return estimateEquals + estimateInEquals;
    	case NOT_EQUALS:
    		return 1.0 - estimateEquals;
    	default: 
    		return -1.0;
    	}
        
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
    	StringBuilder sb = new StringBuilder();
    	for(int i =0;i < this.numBuckets ;i++)
    	{
    		sb.append("bucket["+i+"]:"+bucketArray[i]);
    	}
    	
        return null;
    }
}
