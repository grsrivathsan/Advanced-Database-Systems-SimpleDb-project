package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private boolean flag = false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.t = t;
    	this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type[] t = new Type[1];
    	t[0] = Type.INT_TYPE;
    	TupleDesc tdesc = new TupleDesc(t);
        return tdesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	if(child == null) throw new DbException("Child NULL");
    	child.open();
    	super.open();
    	flag = false;
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    	flag = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	flag = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	int deleteCount = 0;
    	if(flag || child == null)
    		return null;
    	while(child.hasNext())
    	{
    		Tuple dt = child.next();
    		try {
				Database.getBufferPool().deleteTuple(t, dt);
				deleteCount = deleteCount + 1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    	}
    	
    	Tuple tuple = new Tuple(getTupleDesc());
    	flag = true;
    	tuple.setField(0,new IntField(deleteCount));
        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] children = new DbIterator[1];
    	children[0] = child;
    	return children;
        //return null;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	child = children[0];
    }

}
