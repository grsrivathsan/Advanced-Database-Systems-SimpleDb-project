package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hamcrest.core.IsInstanceOf;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    Map<PageId,Integer> pageId_tno = new HashMap<PageId,Integer>();
    public PageId pid;
    public int tupleno;
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
    	//pageId_tno.put(pid, tupleno);
    	this.pid = pid;
    	this.tupleno = tupleno;
    	
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
    	return tupleno;
        //return 0;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
    	return pid;
        //return null;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
    	if(o != null)
    	{	
    	 if (o instanceof RecordId) {
			RecordId r = (RecordId) o;
			if((r.tupleno == this.tupleno) && (r.getPageId().equals(this.pid)))
					{
				      return true;
					}
			
		    } 
        }
    	
        //throw new UnsupportedOperationException("implement this");
		return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
    	List<Integer> l = new ArrayList<Integer>();
    	l.add(this.getPageId().hashCode());
    	l.add(this.tupleno());
    	//Computer hash code for array list
    	return Arrays.hashCode(l.toArray());
    	
    	/*int a[] = new int[2];
    	a[0] = this.getPageId().hashCode();
    	a[1] = this.tupleno();
    	//To convert Arrays into hashcode
    	return Arrays.hashCode(a);*/
    	
        //throw new UnsupportedOperationException("implement this");

    }

}
