package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */


public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	public File f;
	public TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return f.hashCode();
       // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return td;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	HeapPage p = null;
    	long position = pid.pageNumber() * BufferPool.getPageSize();
    	RandomAccessFile randomAccess = null;
		try {
			randomAccess = new RandomAccessFile(f, "r");
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	try {
			randomAccess.seek(position);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	byte[] buffer = new byte[BufferPool.getPageSize()];
    	try {
			randomAccess.read(buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	try {
			p = new HeapPage((HeapPageId) pid, buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
        return p;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	return (int) (f.length() / BufferPool.getPageSize());
       //return 0;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1|lab2
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1|lab2
    }
    
    public class HeapFileIterator implements DbFileIterator{
    	int currentPage;
        Iterator<Tuple> currentIterator = null;
        TransactionId tid;
        boolean open = false;
        
        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }
        
        @Override
        public void open() throws DbException, TransactionAbortedException {
            open = true;
            currentPage = 0;
            if (currentPage >= numPages()) {
                return;
            }
			else {
            currentIterator = ((HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(),currentPage), Permissions.READ_ONLY)).iterator();
            while (!currentIterator.hasNext()) {
                currentPage++;
                if (currentPage < numPages()) {
                    currentIterator = ((HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(), currentPage),Permissions.READ_ONLY)).iterator();
                } else {
                    break;
                }
            }
			}
			
        }
		
        @Override
        public boolean hasNext() throws DbException,TransactionAbortedException {
            if (!open) {
                return false;
            }
            
            else if(currentPage < numPages())
            {
            	return true;
            }
            
            else 
            {
            	return false;
            }
			
          
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException,NoSuchElementException {
            
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Tuple result = currentIterator.next();
            while (!currentIterator.hasNext()) {
                currentPage++;
                if (currentPage < numPages()) {
                    currentIterator = ((HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(), currentPage),Permissions.READ_ONLY)).iterator();
                } else {
                    break;
                }
            }
			
            return result;
        }
		@Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!open) {
                throw new DbException("");
            }
            close();
            open();          
        }

        @Override
        public void close() {
            currentIterator = null;
            currentPage = 0;
            open = false;
        }

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	 return new HeapFileIterator(tid);
    }
    
    

}

