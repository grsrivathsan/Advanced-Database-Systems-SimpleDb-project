package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    //by default num pages equal to default pages. can be overridden by constructor
    public int maxPages = DEFAULT_PAGES;
    //Since pageID,Page combination is Unique we are using HashTable
   // public Hashtable<PageId, Page> pageId_page;
    public HashMap<PageId, Page> pageId_page;
    
    public ArrayList<PageId> pageId_fifo;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     * @throws DbException 
     */
    public BufferPool(int numPages)  {
        // some code goes here
    	/*if(numPages > DEFAULT_PAGES)
    		throw new DbException("Exceeds Max Page request of "+DEFAULT_PAGES);*/
    	//pageId_page = new Hashtable<PageId,Page>();
    	pageId_page = new HashMap<PageId,Page>();
    	maxPages = numPages;
    	//for maintaining fifo order during page eviction
    	pageId_fifo = new ArrayList<PageId>();
    	
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	Page page = null;
    	pageId_fifo.add(pid);
    	if(pageId_page.containsKey(pid))
    	{
    		page = pageId_page.get(pid);
    		//pageId_fifo.add(pid);
    	}
    	else {
    		if(pageId_page.size() < maxPages )
    		{		
    		    page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    		    pageId_page.put(pid, page);
    		    //pageId_fifo.add(pid);
    		}
    		else
    		{
    			//we have to evict one page
    			page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    			evictPage();
    			pageId_page.put(pid, page);
    		    //pageId_fifo.add(pid);
    			//throw new DbException("Buffer pool reached its max size of "+DEFAULT_PAGES);
    		}
    	}
    	
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	ArrayList<Page> dirtyPages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        
    	for(int i = 0;i < dirtyPages.size();i++)
    	{
    		PageId pageId = dirtyPages.get(i).getId();
    		if(pageId_page.containsKey(pageId))
    		{
    			pageId_page.get(pageId).markDirty(true, tid);
    			
    		}
    		else
    		{
    			if(pageId_page.size() == maxPages) evictPage();
    			pageId_page.put(pageId, dirtyPages.get(i));
    			pageId_page.get(pageId).markDirty(true, tid);
    		}
    		
    	}
    	
    /*	DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dpList = f.insertTuple(tid, t);
        // Now let's insert all dirty pages back to BufferPool
        for (Page p : dpList) {
            PageId pid = p.getId();
            if (!pageId_page.containsKey(pid) && pageId_page.size() == maxPages) evictPage();
            pageId_page.put(pid, p);
            pageId_page.get(pid).markDirty(true, tid);
        }*/
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	
    	ArrayList<Page> dirtyPages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
      
    	for(int i = 0;i < dirtyPages.size();i++)
    	{
    		PageId pageId = dirtyPages.get(i).getId();
    		if(pageId_page.containsKey(pageId))
    		{
    			pageId_page.get(pageId).markDirty(true, tid);
    			
    		}
    		else
    		{
    			if(pageId_page.size() == maxPages) evictPage();
    			pageId_page.put(pageId, dirtyPages.get(i));
    			pageId_page.get(pageId).markDirty(true, tid);
    		}
    	}
      
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	for(PageId p : pageId_page.keySet())
    	{
    		if(pageId_page.get(p).isDirty() != null)
    			flushPage(p);
    	}
    		

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    	pageId_page.remove(pid);
    	
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	Page p = pageId_page.get(pid);
    	Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
    	p.markDirty(false, null);
    	
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
    	//logic: find the clean pages. if only one clean page evict that
    	//else evict the pages in fifo order using pid array list    	
        // some code goes here
        // not necessary for lab1
    	
    	ArrayList<PageId> cleanPge = new ArrayList<PageId>();
    	for(PageId p : pageId_page.keySet())
    	{
    		if(pageId_page.get(p).isDirty() == null)
    			cleanPge.add(p);
    	}
    	
    	if(!cleanPge.isEmpty())
    	{
    		PageId removePid = cleanPge.get(0);
    		//Take the first inserted element in dirtyPages array (FIFO)
    		try {
    			//evict pages in fifo order
				  flushPage(cleanPge.get(0));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		//remove the pages after flush operation
    		//pageId_page.remove(removePid);
    		discardPage(removePid);
    		cleanPge.remove(removePid);
    	}
    	else
    	{
    		//If there are no clean pages, then evict the first in clean page thats not used
    		//System.out.println("no dirty pages");
    		try {
				flushPage(pageId_fifo.get(0));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    		//pageId_page.remove(pageId_fifo.get(0));
    		discardPage(pageId_fifo.get(0));
    		pageId_fifo.remove(0);
    		
    	}
    
    
    }

}
