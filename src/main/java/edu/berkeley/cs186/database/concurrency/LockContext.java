package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import org.omg.CORBA.DynAnyPackage.Invalid;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    // Core API (Self-implemented functions) ////////////////////////////////////////////////////////////////

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {

        if (this.readonly) {
            throw new UnsupportedOperationException("Lock context is ReadOnly");
        }
        if (!this.lockman.getLockType(transaction, getResourceName()).equals(LockType.NL)) {
            throw new DuplicateLockRequestException("Transaction already holds a lock");
        }
        //Check for valid parent-child relationship
        LockContext parent = this.parentContext();
        LockType parentType = null;
        while(parent != null) {
            parentType = this.parentContext().getExplicitLockType(transaction);
            if (!parentType.equals(LockType.NL)) {
                break;
            }
            parent = parent.parentContext();
        }
        if (parent != null && parentType != null && !LockType.canBeParentLock(parentType, lockType)) {
            throw new InvalidLockException("Invalid lock combination with parent");
        }
        //Acquire lock
        this.lockman.acquire(transaction, getResourceName(), lockType);
        //Update parent's numChildLocks
        parent = this.parentContext();
        while (parent != null) {
                int numLocks = parent.getNumChildren(transaction);
                parent.numChildLocks.put(transaction.getTransNum(), numLocks + 1);
                parent = parent.parentContext();
        }
        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {

        if (this.readonly) {
            throw new UnsupportedOperationException("Lock context is ReadOnly");
        }
        if (this.lockman.getLockType(transaction, getResourceName()).equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held by transaction");
        }
        if (getNumChildren(transaction) != 0) {
            throw new InvalidLockException("Invalid release. Still has children locks");
        }
        //Release lock
        this.lockman.release(transaction, getResourceName());
        //Update parent's numChildLocks
        LockContext parent = this.parentContext();
        while (parent != null) {
            int numLocks = parent.getNumChildren(transaction);
            parent.numChildLocks.put(transaction.getTransNum(), numLocks - 1);
            parent = parent.parentContext();
        }
        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {

        if (this.readonly) {
            throw new UnsupportedOperationException("Lock context is ReadOnly");
        }
        if (this.lockman.getLockType(transaction, getResourceName()).equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held by transaction");
        }
        if (this.lockman.getLockType(transaction, getResourceName()).equals(newLockType)) {
            throw new DuplicateLockRequestException("Transaction already holds newLockType lock");
        }
        LockType oldLockType = this.lockman.getLockType(transaction, getResourceName());

        if (newLockType.equals(LockType.SIX)) {
            if (!hasSIXAncestor(transaction) && (oldLockType.equals(LockType.IS) || oldLockType.equals(LockType.IX)
            || oldLockType.equals(LockType.S))) {
                //Valid promotion with SIX, all S and IS locks on descendants must be released
                List<ResourceName> sisDesc = sisDescendants(transaction);
                for (ResourceName rName : sisDesc) {
                    LockContext parent = fromResourceName(this.lockman, rName).parentContext();
                    while (parent != null) {
                        int numLocks = parent.getNumChildren(transaction);
                        parent.numChildLocks.put(transaction.getTransNum(), numLocks - 1);
                        parent = parent.parentContext();
                    }
                }
                //Add current resource lock to be released with others, newLockType will be acquired
                sisDesc.add(getResourceName());
                this.lockman.acquireAndRelease(transaction, getResourceName(), newLockType, sisDesc);
            } else {
                throw new InvalidLockException("Invalid promotion to SIX");
            }
        } else if (LockType.substitutable(newLockType, oldLockType) && !newLockType.equals(oldLockType)) {
            //Not SIX, valid promotion
            this.lockman.promote(transaction, getResourceName(), newLockType);
        } else {
            //Not SIX, NOT valid promotion
            throw new InvalidLockException("Invalid promotion");
        }
        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {

        if (this.readonly) {
            throw new UnsupportedOperationException("Lock context is ReadOnly");
        }
        LockType currLockType = this.lockman.getLockType(transaction, getResourceName());
        if (currLockType.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held by transaction");
        }
        if (currLockType.equals(LockType.S) || currLockType.equals(LockType.X)) {
            return;
        }

        List<ResourceName> resourceList = new ArrayList<>();
        //Iterate through all locks belonging to this transaction and check if current resource's descendant
        List<Lock> allLocks = this.lockman.getLocks(transaction);
        for (Lock l : allLocks) {
            if (l.name.isDescendantOf(this.getResourceName()) && !fromResourceName(lockman, l.name).equals(this)) {
                resourceList.add(l.name);
            }
        }
        //For each child, decrement all ancestor's numChildLocks
        for (ResourceName rName : resourceList) {
            LockContext childLock = fromResourceName(this.lockman, rName);
            LockContext parent = childLock.parentContext();
            while (parent != null) {
                int numLocks = parent.getNumChildren(transaction);
                parent.numChildLocks.put(transaction.getTransNum(), numLocks - 1);
                parent = parent.parentContext();
            }
        }
        LockType newLockType;
        if (currLockType.equals(LockType.IS)) {
            newLockType = LockType.S;
        } else {
            //IX and SIX case
            newLockType = LockType.X;
        }
        resourceList.add(getResourceName()); //Add currently held lock to release list
        this.lockman.acquireAndRelease(transaction, getResourceName(), newLockType, resourceList);

        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;

        return this.lockman.getLockType(transaction, this.name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;

        //Explicit lock exists
        if (!getExplicitLockType(transaction).equals(LockType.NL)) {
            return getExplicitLockType(transaction);
        }
        //Check my parents
        LockContext prevParent = this.parentContext();
        while(prevParent != null) {
            LockType currLockType = prevParent.getExplicitLockType(transaction);
            if (!currLockType.equals(LockType.NL)) {
                if (currLockType.equals(LockType.IS) || currLockType.equals(LockType.IX)) {
                    return LockType.NL;
                } else if (currLockType.equals(LockType.SIX)) {
                    return LockType.S;
                } else {
                    return currLockType;
                }
            }
            prevParent = prevParent.parentContext();
        }
        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // 
        LockContext parentLock = this.parentContext();
        while(parentLock != null) {
            if (this.lockman.getLockType(transaction, parentLock.getResourceName()).equals(LockType.SIX)) {
                return true;
            }
            parentLock = parentLock.parentContext();
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {

        List<ResourceName> resourceList = new ArrayList<>();
        //Iterature through all locks belonging to this transaction and check their type
        List<Lock> allLocks = this.lockman.getLocks(transaction);
        for (Lock l : allLocks) {
            if (l.lockType.equals(LockType.S) || l.lockType.equals(LockType.IS)) {
                if (l.name.isDescendantOf(this.getResourceName()) && !fromResourceName(lockman, l.name).equals(this)) {
                    resourceList.add(l.name);
                }
            }
        }
        return resourceList;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

