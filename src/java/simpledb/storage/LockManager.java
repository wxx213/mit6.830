package simpledb.storage;

import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private Map<PageId, Lock> lockMap;

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
    }

    public synchronized boolean acquireLock(PageId pageId, TransactionId tid, LockType type) {
        Lock lock = this.lockMap.get(pageId);
        // the lock for pageId not exist
        if (lock == null) {
            lock = new Lock(type);
            lock.addTransactionId(tid);
            this.lockMap.put(pageId, lock);
            return true;
        }

        // the lock for pageId exist and the type is not equal
        if (type != lock.lockType) {
            if (lock.transactionIdHashSet.size() == 1 && lock.transactionIdHashSet.contains(tid)) {
                return true;
            }
            return false;
        }

        // the lock for pageId exist and the type is equal
        if (lock.lockType == LockType.WRITE_LOCK && !lock.transactionIdHashSet.contains(tid)) {
            return false;
        }
        return true;
    }

    public synchronized boolean releaseLock(PageId pageId, TransactionId tid) {
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

    public synchronized boolean holdLock(PageId pageId, TransactionId tid) {
        Lock lock = this.lockMap.get(pageId);
        // the lock for pageId not exist
        if (lock == null) {
            return false;
        }
        return lock.transactionIdHashSet.contains(tid);
    }
}
