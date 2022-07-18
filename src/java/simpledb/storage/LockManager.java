package simpledb.storage;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private Map<PageId, Lock> lockMap;
    private Map<TransactionId, HashSet<PageId>> mapTidPages;
    private List<List<TransactionId>> transactionWaitGraph;
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
        this.transactionWaitGraph = new ArrayList<>();
    }

    private void transactionWaitGraphAdd(PageId pageId, TransactionId tid) throws TransactionAbortedException {
        Lock lock = this.lockMap.get(pageId);
        if (lock == null) {
            return;
        }
        List<TransactionId> nodes = new ArrayList<>();
        List<TransactionId> nodeList = null;
        for (int i=0;i<this.transactionWaitGraph.size();i++) {
            nodeList = this.transactionWaitGraph.get(i);
            TransactionId node = nodeList.get(0);
            nodes.add(node);
        }

        nodeList = null;
        for (int i=0;i<this.transactionWaitGraph.size();i++) {
            nodeList = this.transactionWaitGraph.get(i);
            TransactionId node = nodeList.get(0);
            if (tid.equals(node)) {
                break;
            }
        }
        if (nodeList == null) {
            nodeList = new ArrayList<>();
            nodeList.add(tid);
            this.transactionWaitGraph.add(nodeList);
            nodes.add(tid);
        }

        for (TransactionId waitTid : lock.transactionIdHashSet) {
            if (!nodeList.contains(waitTid)) {
                nodeList.add(waitTid);
            }
            if (!nodes.contains(waitTid)) {
                List<TransactionId> newList = new ArrayList<>();
                newList.add(waitTid);
                this.transactionWaitGraph.add(newList);
            }
        }

        // check cycle and throw exception.
        if (checkCycleForGraph(this.transactionWaitGraph, tid)) {
            throw new TransactionAbortedException();
        }
    }

    private boolean checkCycleForGraph(List<List<TransactionId>> graph, TransactionId node) {
        // clone the graph.
        List<List<TransactionId>> clonedGraph = new ArrayList<>();
        for (List<TransactionId> list : graph) {
            List<TransactionId> listCloned = new ArrayList<>();
            for (TransactionId tid : list) {
                listCloned.add(tid);
            }
            clonedGraph.add(listCloned);
        }

        // save the size before modified.
        int graphSize = clonedGraph.size();
        List<TransactionId> topologySort = findZeroIndegreeNode(clonedGraph);

        // if the node is in topology sort list, the node don't belong to a cycle.
        if (topologySort.contains(node)) {
            return false;
        }
        return true;
    }

    private List<TransactionId> findZeroIndegreeNode(List<List<TransactionId>> graph) {
        List<TransactionId> result = new ArrayList<>();
        List<TransactionId> indegreeNoZero = new ArrayList<>();

        if (graph.size() == 0) {
            return new ArrayList<>();
        }
        // all the nodes in the graph.
        List<TransactionId> nodeList = new ArrayList<>();

        // find all the nozero indegree nodes;
        for (List<TransactionId> list : graph) {
            nodeList.add(list.get(0));
            for (int i=1;i<list.size();i++) {
                TransactionId tid = list.get(i);
                if (!indegreeNoZero.contains(tid)) {
                    indegreeNoZero.add(tid);
                }
            }
        }

        // find all the zero indegree nodes and remove them in the current grph.
        List<TransactionId> indegreeZero = new ArrayList<>();
        for (TransactionId tid : nodeList) {
            if (!indegreeNoZero.contains(tid)) {
                result.add(tid);
                transactionWaitGraphRemove(graph, tid);
            }
        }

        // find all the zero indegree nodes in the subgraph.
        List<TransactionId> subResult = null;
        if (result.size() > 0) {
            subResult = findZeroIndegreeNode(graph);
        }

        // merge the result.
        if (subResult != null) {
            for (TransactionId tid : subResult) {
                result.add(tid);
            }
        }
        return result;
    }

    private void transactionWaitGraphRemove(List<List<TransactionId>> graph, TransactionId tid) {
        List<TransactionId> nodeList = null, targetNodeList = null;

        for (int i=0;i<graph.size();i++) {
            nodeList = graph.get(i);
            TransactionId node = nodeList.get(0);
            if (tid.equals(node)) {
                if (targetNodeList == null) {
                    targetNodeList = nodeList;
                }
            } else {
                nodeList.remove(tid);
            }
        }

        if (targetNodeList != null) {
            graph.remove(targetNodeList);
        }
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

    public synchronized boolean acquireLock(PageId pageId, TransactionId tid, LockType type) throws TransactionAbortedException {
        //System.out.printf("%d, %d, %d, %s\n", tid.getId(), pageId.getTableId(),
        //        pageId.getPageNumber(), type.toString());

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
                // if the page is hold by only one transaction, change read lock to write lock
                if (type == LockType.WRITE_LOCK) {
                    lock.lockType = LockType.WRITE_LOCK;
                }
                lock.addTransactionId(tid);
                transactionAddPage(tid, pageId);
                return true;
            }
            transactionWaitGraphAdd(pageId, tid);
            return false;
        }

        // the lock for pageId exist and the type is equal
        if (lock.lockType == LockType.WRITE_LOCK && !lock.transactionIdHashSet.contains(tid)) {
            transactionWaitGraphAdd(pageId, tid);
            return false;
        }
        lock.addTransactionId(tid);
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
        transactionWaitGraphRemove(this.transactionWaitGraph, tid);
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
        transactionWaitGraphRemove(this.transactionWaitGraph, tid);
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
