package simpledb.storage;

import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private Map<PageId, Lock> lockMap;
    private Map<TransactionId, HashSet<PageId>> mapTidPages;

    public  enum LockType {
        READ_LOCK, WRITE_LOCK
    }

    private class Lock {
        public LockType lockType;
        public HashSet<TransactionId> transactionIdHashSet;
        public Lock(LockType type) {
            this.lockType = type;
            this.transactionIdHashSet = new HashSet<>();
        }
        public void addTransactionId(TransactionId tid) {
            if (!this.transactionIdHashSet.contains(tid)) {
                this.transactionIdHashSet.add(tid);
            }
        }
        public void deleteTransactionId(TransactionId tid) {
            this.transactionIdHashSet.remove(tid);
        }
        public boolean transactionIdExist(TransactionId tid) {
            return this.transactionIdHashSet.contains(tid);
        }
    }

    public LockManager() {
        this.lockMap = new ConcurrentHashMap<>();
        this.mapTidPages = new ConcurrentHashMap<>();
    }

    private void transactionAddPage(TransactionId tid, PageId pageId) {
        HashSet<PageId> pageIds = this.mapTidPages.get(tid);
        if(pageIds == null) {
            pageIds = new HashSet<>();
            this.mapTidPages.put(tid, pageIds);
        }
        if (!pageIds.contains(pageId)) {
            pageIds.add(pageId);
        }
    }
    private void transactionRemovePage(TransactionId tid, PageId pageId) {
        HashSet<PageId> pageIds = this.mapTidPages.get(tid);
        if(pageIds != null) {
            if (pageIds.contains(pageId)) {
                pageIds.remove(pageId);
            }
            if (pageIds.size() == 0) {
                this.mapTidPages.remove(tid);
            }
        }
    }

    public synchronized boolean tryAcquireLock(PageId pageId, TransactionId tid, LockType type, int timeoutMs) {
        long start = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() - start >= timeoutMs) {
                return false;
            }
            if (acquireLock(pageId, tid, type)) {
                return true;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    public synchronized boolean acquireLock(PageId pageId, TransactionId tid, LockType type) {
        Lock lock = this.lockMap.get(pageId);
        // the lock for pageId not exist
        if (lock == null) {
            lock = new Lock(type);
            lock.addTransactionId(tid);
            this.lockMap.put(pageId, lock);
            transactionAddPage(tid, pageId);
            return true;
        }

        // the lock for pageId exist and the type is not equal
        if (type != lock.lockType) {
            if (lock.transactionIdHashSet.size() == 1 && lock.transactionIdHashSet.contains(tid)) {
                transactionAddPage(tid, pageId);
                return true;
            }
            return false;
        }

        // the lock for pageId exist and the type is equal
        if (lock.lockType == LockType.WRITE_LOCK && !lock.transactionIdHashSet.contains(tid)) {
            return false;
        }
        transactionAddPage(tid, pageId);
        return true;
    }

    private boolean releaseLockOnly(PageId pageId, TransactionId tid) {
        Lock lock = this.lockMap.get(pageId);
        // the lock for pageId not exist
        if (lock == null) {
            return true;
        }
        if (lock.transactionIdHashSet.contains(tid)) {
            lock.transactionIdHashSet.remove(tid);
            if (lock.transactionIdHashSet.size() == 0) {
                this.lockMap.remove(pageId);
            }
        }
        return true;
    }
    public synchronized boolean releaseLock(PageId pageId, TransactionId tid) {
        boolean res = releaseLockOnly(pageId, tid);
        transactionRemovePage(tid, pageId);
        return res;
    }

    public synchronized boolean releaseLock(TransactionId tid) {
        HashSet<PageId> pageIds = this.mapTidPages.get(tid);
        if(pageIds != null) {
            for (PageId pageId : pageIds) {
                releaseLockOnly(pageId, tid);
            }
            this.mapTidPages.remove(tid);
        }

        return true;
    }

    public synchronized boolean holdLock(PageId pageId, TransactionId tid) {
        Lock lock = this.lockMap.get(pageId);
        // the lock for pageId not exist
        if (lock == null) {
            return false;
        }
        return lock.transactionIdHashSet.contains(tid);
    }

    public synchronized List<PageId> holdLockPages(TransactionId tid) {
        List<PageId> pageIdList = new ArrayList<>();
        HashSet<PageId> pageIdHashSet = this.mapTidPages.get(tid);
        if (pageIdHashSet != null) {
            for (PageId pageId : pageIdHashSet) {
                pageIdList.add(pageId);
            }
        }
        return pageIdList;
    }
}
