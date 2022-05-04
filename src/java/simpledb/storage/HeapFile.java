package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private int heapFileId;
    private TupleDesc tupleDesc;
    private File file;
    private RandomAccessFile randomAccessFile;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        heapFileId = f.getAbsoluteFile().hashCode();
        tupleDesc = td;
        file = f;
        try {
            randomAccessFile = new RandomAccessFile(this.file, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return null;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return heapFileId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pos = BufferPool.getPageSize() * pid.getPageNumber();
        byte[] pageData = new byte[BufferPool.getPageSize()];
        try {
            randomAccessFile.seek(pos);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            randomAccessFile.read(pageData, 0, pageData.length);
            HeapPage heapPage = new HeapPage((HeapPageId) pid, pageData);
            return heapPage;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    private class HeapFileIterator implements DbFileIterator {
        private int numPages;
        private int currentPageNo;
        private int tableId;
        private Iterator<Tuple> currentTupleIterator;

        public HeapFileIterator(int pages, int id) {
            this.numPages = pages;
            this.tableId = id;
            this.currentPageNo = 0;
            this.currentTupleIterator = null;
        }

        public void updateTupleIterator() {
            if (currentPageNo < numPages) {
                HeapPageId pid = new HeapPageId(this.tableId, currentPageNo++);
                HeapPage page = (HeapPage) readPage(pid);
                currentTupleIterator = page.iterator();
            }
        }

        @Override
        public void open() {
            if (currentTupleIterator == null) {
                updateTupleIterator();
            }
        }

        @Override
        public boolean hasNext() {
            if (currentTupleIterator == null) {
                return false;
            }
            if (currentPageNo < numPages) {
                if (currentTupleIterator.hasNext() == true) {
                    return true;
                } else {
                    updateTupleIterator();
                    return hasNext();
                }
            } else {
                return currentTupleIterator.hasNext();
            }
        }

        @Override
        public Tuple next() {
            if (currentTupleIterator == null) {
                throw new NoSuchElementException();
            }
            return currentTupleIterator.next();
        }

        @Override
        public void rewind() {

        }

        @Override
        public void close() {
            if (currentTupleIterator != null) {
                currentTupleIterator = null;
            }
        }
    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this.numPages(), this.getId());
    }

}

