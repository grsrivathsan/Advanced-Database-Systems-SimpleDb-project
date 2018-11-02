package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    HashMap<Field,Integer> count = new HashMap<Field, Integer>();
    HashMap<Field,Integer> aggData = new HashMap<Field,Integer>();
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
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
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
    	
    Field tupleGroupByField = null;
    if(gbfield != Aggregator.NO_GROUPING)
	  tupleGroupByField = tup.getField(gbfield);
    if (!aggData.containsKey(tupleGroupByField))
    {
        int value = 0; //System.out.println(value);
        if(what.equals(simpledb.Aggregator.Op.MIN))        	
        	value = Integer.MAX_VALUE;
        else if(what.equals(simpledb.Aggregator.Op.MAX))
        	value = Integer.MIN_VALUE;
        else if(what.equals(simpledb.Aggregator.Op.AVG))
        	value = 0;
        else
        	value = 0;
      
    		aggData.put(tupleGroupByField, value);    		
    		count.put(tupleGroupByField, 0);
    }
    	
    	int tupleValue = ((IntField) tup.getField(afield)).getValue();
    	int currentValue = aggData.get(tupleGroupByField);
    	int currentCount = count.get(tupleGroupByField);
    	int newValue = currentValue;
    	switch(what)
    	{
    		case MIN: 
    			newValue = (tupleValue > currentValue) ? currentValue : tupleValue;
    			break;
    		case MAX:
    			newValue = (tupleValue < currentValue) ? currentValue : tupleValue;
    			break;
    		case SUM: case AVG:
    			count.put(tupleGroupByField, currentCount+1);
    			newValue = tupleValue + currentValue;
    			break;
    		case COUNT:
    			newValue = currentValue + 1;
    			break;
			default:
				break;
    	}
    	aggData.put(tupleGroupByField, newValue);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
    	//Tuple addMe;
    	for (Field group : aggData.keySet())
    	{
    		int aggregateVal;
    		if (what == Op.AVG)
    		{
    			aggregateVal = aggData.get(group) / count.get(group);
    		}
    		else
    		{
    			aggregateVal = aggData.get(group);
    		}
    		t = new Tuple(tupledesc);
    		if (gbfield == Aggregator.NO_GROUPING){
    			t.setField(0, new IntField(aggregateVal));
    		}
    		else {
        		t.setField(0, group);
        		t.setField(1, new IntField(aggregateVal));    			
    		}
    		tuples.add(t);
    	}
    	return new TupleIterator(tupledesc, tuples);
    }
    	
    	
    

}
