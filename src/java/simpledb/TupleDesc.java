package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    public List<TDItem> TDItemsList = new ArrayList<TDItem>();
     /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
		//TDItem t ;
    	//Iterator iterator = t.iterator();
    	return TDItemsList.iterator();
        // some code goes here
        
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	for(int i = 0;i < typeAr.length;i++) {
    		TDItem tdi = new TDItem(typeAr[i],fieldAr[i]);
    		TDItemsList.add(tdi);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	for(int i=0;i< typeAr.length;i++)
    	{
    		TDItem tdi = new TDItem(typeAr[i],new String());
    		TDItemsList.add(tdi);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
    	int len = TDItemsList.size();
        return len;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	//String fieldName = null;
    	if(i < 0 || i > TDItemsList.size())
    	{
    		throw new NoSuchElementException("Invalid Index " +i);
    	}
    	for(int index = 0;index <TDItemsList.size();index++)
    	{
    		if(i == index)
    		{
    			return TDItemsList.get(i).fieldName;
    		}
    	}
        return null;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if((i < 0) || (i > TDItemsList.size()))
    		throw new NoSuchElementException("Invalid Index "+i);
    	else {
    	for(int index = 0;index < TDItemsList.size();index ++)
    	{
    		if(index == i)
    		{
    			return TDItemsList.get(index).fieldType;
    		}
    	}
    	}
        return null;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	int flag = 0;
    	for(int i = 0;i< TDItemsList.size();i++) {
    		if(TDItemsList.get(i).fieldName.equals(name))
    		{
    			flag = 1;
    			return i;
    		}
    	}
    	
    	if(flag == 0)
    	{
    		throw new NoSuchElementException("No field with name " +name+" found");
    	}
        return 0;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int size = 0;
    	//System.out.println("tdsizze:"+TDItemsList.size());
    	//System.out.println("getlen:"+TDItemsList.get(2).fieldType.getLen());
    	for(int i=0;i<TDItemsList.size();i++)
    	{
    		//System.out.println("Size before:"+size);
    		//System.out.println("getlen:"+TDItemsList.get(i).fieldType.getLen());
    		 size = size + TDItemsList.get(i).fieldType.getLen();
    		//System.out.println("Size after:"+size);
    	}
    	//System.out.println("totalSize:"+size);
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	 Type[] fieldType;
    	 String[] fieldName;
    	 //System.out.println("Within merge");
    	 /*for(int i = 0;i < td1.numFields() ; i ++)
         {
         	System.out.println("td1 field type:"+td1.getFieldType(i));
         	System.out.println("td1 field Name:"+td1.getFieldName(i));
         }*/
    	 /*for(int i = 0;i < td2.numFields() ; i ++)
         {
         	System.out.println("td2 field type:"+td2.getFieldType(i));
         	System.out.println("td2 field Name:"+td2.getFieldName(i));
         }*/
    	
    	int totFields = td1.numFields() + td2.numFields();
    	/* System.out.println("td1.numFields:"+td1.numFields());
    	 System.out.println("td2.numFields:"+td2.numFields());
    	 System.out.println("totFields:"+totFields);*/
    	fieldType = new Type[totFields];
    	fieldName = new String[totFields];
    	for(int i = 0;i<td1.numFields();i++)
    	{ 
    		//System.out.println("i:"+i);
    		fieldType[i] = td1.getFieldType(i);
    		fieldName[i] = td1.getFieldName(i);
    	}
    	int mergeParameter = td1.numFields();
    	for(int j = 0;j < td2.numFields();j++)
    	{
    		//System.out.println("j:"+j);
    		fieldType[mergeParameter] = td2.getFieldType(j);
    		fieldName[mergeParameter] = td2.getFieldName(j);
    		mergeParameter = mergeParameter + 1;
    	}
    	
    	
    	
    	TupleDesc td3 = new TupleDesc(fieldType,fieldName);
    	/*System.out.println("td3.numFields:"+td3.numFields());
    	 for(int i = 0;i < td3.numFields();i++) {
    		 System.out.println("mergeType:"+td3.getFieldType(i));
    		 System.out.println("mergedFieldName:"+td3.getFieldName(i));
    	 }*/
    		 
    	 
        return td3;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if((o == null) || !(o instanceof TupleDesc))
        {
        	return false;
        }
       
    	TupleDesc tdo = (TupleDesc) o;
    
    	int count = 0;
    	if(this.numFields() == tdo.numFields())
    	{
    		for(int i=0;i<this.numFields();i++)
    		{
    			if(this.getFieldType(i) == tdo.getFieldType(i))
    			{
    				count = count + 1;
    			}
    		}
    	}
    	
    	if(count == this.numFields())
    	{
    		return true;
    	}
    	
    	return false;
    	
        
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	StringBuilder sb = new StringBuilder();
    	
    	for(int i=0;i<TDItemsList.size();i++)
    	{
    		sb.append(TDItemsList.get(i).fieldName+","+TDItemsList.get(i).fieldType);
    	}
        return sb.toString();
    }
}
