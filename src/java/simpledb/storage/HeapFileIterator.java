package simpledb.storage;

import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class HeapFileIterator implements DbFileIterator {

    private final HeapFile heapFile;
    private final TransactionId tid;
    private int currPageIndex;
    private boolean isOpen;
    private Iterator<Tuple> currTupleIterator;

    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
        this.currPageIndex = -1; // Iterator is not open yet
        this.isOpen = false;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        isOpen = true;
        currPageIndex = 0;
        currTupleIterator = getPageIterator(currPageIndex);
    }

    private Iterator<Tuple> getPageIterator(int pageIndex) throws TransactionAbortedException, DbException {
        // validate pageIndex
        if (pageIndex < 0 || pageIndex >= heapFile.numPages()) {
            return null;
        }

        // one table corresponds to one heap file: use fileId as tableId
        HeapPageId pid = new HeapPageId(heapFile.getId(), pageIndex);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);

        return page.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!isOpen) {
            return false;
        }
        while (true) {
            // check the current tuple iterator
            if (currTupleIterator == null) return false;
            // check the current tuple iterator (current page) has left tuples
            if (currTupleIterator.hasNext()) {
                return true;
            }
            // if current iterator finished, move the next page
            currPageIndex++;
            // check if the page index move out of boundary
            if (currPageIndex >= heapFile.numPages()) {
                return false;
            }
            // update the current page iterator
            currTupleIterator = getPageIterator(currPageIndex);
        }
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException("There is no more tuple.");
        }
        return currTupleIterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        if (!isOpen) {
            throw new DbException("Iterator not open");
        }
        close();
        open();
    }

    @Override
    public void close() {
        // the opposite of open()
        isOpen = false;
        currPageIndex = -1;
        currTupleIterator = null;
    }

}