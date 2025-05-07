package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import static org.junit.Assert.assertArrayEquals;

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
    
    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
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
        return this.f;
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
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (!(pid instanceof HeapPageId)) {
            throw new IllegalArgumentException("Expect HeapPageId");
        }
        HeapPageId hpid = (HeapPageId) pid;
        int pageNo = hpid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        byte[] data = new byte[pageSize];

        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "r")) {
            long offset = pageNo * pageSize;
            if (offset + pageSize > raf.length()) {
                throw new IllegalArgumentException("Page number out of bounds");
            }
            raf.seek(offset);
            raf.read(data);
            return new HeapPage(hpid, data);

        } catch (IOException e) {
            throw new RuntimeException("Error reading page from file", e);
        }

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
        int pageSize = BufferPool.getPageSize();
        long fileSize = f.length();
        return (int) fileSize / pageSize;
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

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        private final TransactionId tid;

        private int currPageNo;
        private Iterator<Tuple> tupleIterator;
        private boolean isOpen = false;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        public Iterator<Tuple> getTuplesIterator(int pageNo) throws DbException, TransactionAbortedException {
            if (pageNo < 0 || pageNo >= numPages()) {
                throw new DbException("Page number out of bounds");
            }
            // 1:1 relationship between table and heap file (file id = table id)
            HeapPageId hpid = new HeapPageId(getId(), pageNo);
            // Must use BufferPool to get the page, not readPage
            HeapPage hpage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
            return hpage.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.isOpen = true;
            this.currPageNo = 0;
            this.tupleIterator = getTuplesIterator(currPageNo);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) return false;
            while (currPageNo < numPages()) {
                if (tupleIterator != null && tupleIterator.hasNext()) {
                    // if current page has tuples
                    return true;
                } else {
                    // if current page has no tuples, move to next page
                    currPageNo++;
                    if (currPageNo < numPages()) {
                        tupleIterator = getTuplesIterator(currPageNo); // update iterator
                    }
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException("No more tuples");
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            this.isOpen = false;
            this.currPageNo = 0;
            this.tupleIterator = null;
        }

    }

}

