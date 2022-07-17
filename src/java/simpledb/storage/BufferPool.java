package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int maxNumPages;
    private int currentNumPages;
    private Map<Integer, Map<Integer, Page>> hashPages;
    private LockManager lockManager;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        maxNumPages = numPages;
        currentNumPages = 0;
        hashPages = new HashMap<>();
        lockManager = new LockManager();
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
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        LockManager.LockType lockType;
        if (perm == Permissions.READ_ONLY) {
            lockType = LockManager.LockType.READ_LOCK;
        } else {
            lockType = LockManager.LockType.WRITE_LOCK;
        }
        acquireLockBlock(pid, tid, lockType);

        int tableId = pid.getTableId();
        int pgId = pid.getPageNumber();
        Page page = getCachePage(tableId, pgId);
        if (page != null) {
            return page;
        }
        DbFile heapFile = Database.getCatalog().getDatabaseFile(tableId);
        page = heapFile.readPage(pid);
        if (page == null) {
            return null;
        }
        putCachePage(page, tableId, pgId);
        return page;
    }

    private void acquireLockBlock(PageId pageId, TransactionId tid, LockManager.LockType type) throws TransactionAbortedException {
        while (true) {
            if (this.lockManager.acquireLock(pageId, tid, type)) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    private int getCachePageNumber(int tableId) {
        Map<Integer, Page> tablePages = hashPages.get(tableId);
        if (tablePages != null) {
            return tablePages.size();
        }
        return 0;
    }

    private Page getCachePage(int tableId, int pageId) {
        Map<Integer, Page> tablePages = hashPages.get(tableId);
        if (tablePages != null) {
            Page page = tablePages.get(pageId);
            if (page != null) {
                return page;
            }
        }
        return null;
    }
    private void removeCachePage(int tableId, int pageId) {
        Map<Integer, Page> tablePages = hashPages.get(tableId);
        if (tablePages != null) {
            if (tablePages.containsKey(pageId)) {
                tablePages.remove(pageId);
                currentNumPages--;
            }
        }
    }
    private void putCachePage(Page page, int tableId, int pageId) throws DbException {
        Map<Integer, Page> tablePages = hashPages.get(tableId);

        if (page != null) {
            if (tablePages == null) {
                tablePages = new HashMap<>();
                hashPages.put(tableId, tablePages);
            }
            if (tablePages.get(pageId) == null) {
                if (currentNumPages >= maxNumPages) {
                    evictPage();
                }
                if (currentNumPages >= maxNumPages) {
                    throw new DbException("page cache full");
                }
                tablePages.put(pageId, page);
                currentNumPages++;
            }
        }
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.lockManager.releaseLock(tid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.lockManager.holdLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            discardPages(tid);
        }
        this.lockManager.releaseLock(tid);

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
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);

        List<Page> pageList = heapFile.insertTuple(tid, t);
        for (Page page : pageList) {
            putCachePage(page, tableId, page.getId().getPageNumber());
        }
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
        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pageList = heapFile.deleteTuple(tid, t);
        for (Page page : pageList) {
            putCachePage(page, tableId, page.getId().getPageNumber());
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
        int tableId = pid.getTableId();
        int pgId = pid.getPageNumber();
        removeCachePage(tableId, pgId);
    }

    public synchronized void discardPages(TransactionId tid) {
        List<PageId> pageIdList = this.lockManager.holdLockPages(tid);
        for (PageId pageId : pageIdList) {
            discardPage(pageId);
        }
    }
    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        int tableId = pid.getTableId();
        int pgId = pid.getPageNumber();
        HeapPage heapPage = (HeapPage) getCachePage(tableId, pgId);
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        if (heapPage != null) {
            heapFile.writePage(heapPage);
            if (heapPage.isDirty() != null) {
                heapPage.markDirty(false, null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        List<PageId> pageIdList = this.lockManager.holdLockPages(tid);
        for (PageId pageId : pageIdList) {
            flushPage(pageId);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        List<Page> pageList = new ArrayList<>();
        for (Map<Integer, Page> tablePages : this.hashPages.values()) {
            for (Page page : tablePages.values()) {
                pageList.add(page);
            }
        }
        for (Page page : pageList) {
            if (page.isDirty() == null) {
                discardPage(page.getId());
            }
        }
    }

}
