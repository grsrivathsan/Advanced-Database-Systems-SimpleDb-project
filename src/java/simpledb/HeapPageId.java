package simpledb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

	
	public int tableId;
	public int pgNo;
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
    	this.tableId = tableId;
    	this.pgNo = pgNo;
    	
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
    	return this.tableId;
       
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
        // some code goes here
        return this.pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
    	
     	List<Integer> l = new ArrayList<Integer>();
    	l.add(this.getTableId());
    	l.add(this.pageNumber());
    	return Arrays.hashCode(l.toArray());
    	
    	//To Compute hashCode for an array. Arrays inherit objects
    	//return Arrays.hashCode(a);
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
    	
    	if( o instanceof PageId)
    	{
    		PageId pid = (PageId) o;
    		if((pid.getTableId() == this.getTableId()) && pid.pageNumber() == this.pageNumber())
    		{
    			return true;
    		}
    	}
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
