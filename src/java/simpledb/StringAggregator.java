package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    HashMap<Field,Integer> count = new HashMap<Field, Integer>();

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
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	
    	if (what != Aggregator.Op.COUNT) {
    		throw new IllegalArgumentException(
    			"String aggregation operator only supports COUNT.");
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field tupleGroupByField = null;
    	if(gbfield != Aggregator.NO_GROUPING)
    		tupleGroupByField = tup.getField(gbfield);
             	
    	if (!count.containsKey(tupleGroupByField))
    	{
    		count.put(tupleGroupByField, 0);
    	}
    	
    	int currentCount = count.get(tupleGroupByField);
    	count.put(tupleGroupByField, currentCount+1);
    	
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	TupleDesc tupledesc = null; 
    	Tuple t = null;
    	if(gbfield != Aggregator.NO_GROUPING)
    	{
    		Type[] type = new Type[2];
    		type[0] = gbfieldtype;
    		type[1] = Type.INT_TYPE;
    		tupledesc = new TupleDesc(type);
    	}
    	else
    	{
    		Type[] type = new Type[1];
    		//type[0] = gbfieldtype;
    		type[0] = Type.INT_TYPE;
    		tupledesc = new TupleDesc(type);
    	}    	
    	
    	
    	Iterator<Entry<Field, Integer>> iter = count.entrySet().iterator();
    	while(iter.hasNext())
    	{
    		Map.Entry pair = (Map.Entry)iter.next();
    		int aggregateValue = count.get((Field)pair.getKey());
    		t = new Tuple(tupledesc);
    		if(gbfield == Aggregator.NO_GROUPING)
    		{
    			t.setField(0, new IntField(aggregateValue));
    		}
    		else
    		{
    			t.setField(0, (Field) pair.getKey());
    			t.setField(1, new IntField(aggregateValue));
    		}
    		tuples.add(t);
    	}
    	
    	return new TupleIterator(tupledesc, tuples);
        //throw new UnsupportedOperationException("please implement me for lab3");
    }

}
