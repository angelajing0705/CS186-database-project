package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */

public class LockUtil {

    // CoreAPI (Self-implemented helper functions) ////////////////////////////////////////////////////////////////

    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */

    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        //effectiveLockType can substitute requestType: do nothing (we never reduce permission)
        //This includes when requestType is NL
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }

        //Modify ancestor locks to accomodate for new requestType lock
        ensureAncestors(transaction, parentContext, requestType);

        if (requestType.equals(LockType.S)) {
            switch (explicitLockType) {
                case NL: lockContext.acquire(transaction, LockType.S);
                    break;
                case IS: lockContext.escalate(transaction);
                    break;
                case IX: lockContext.promote(transaction, LockType.SIX);
                    break;
                default: break;
            }
        } else {
            //requestType is X
            switch (explicitLockType) {
                case NL: lockContext.acquire(transaction, LockType.X);
                    break;
                case IS:
                    //First escalate to S, then promote to X (IS not directly promotable to X)
                    lockContext.escalate(transaction);
                    lockContext.promote(transaction, LockType.X);
                    break;
                case S: lockContext.promote(transaction, LockType.X);
                    break;
                case IX:
                case SIX: lockContext.escalate(transaction);
                    break;
                default: break;
            }
        }
        return;
    }

    
    private static void ensureAncestors(TransactionContext transaction, LockContext parentContext, LockType requestType) {
        ArrayList<LockContext> allAncestors = new ArrayList<>();
        while(parentContext != null) {
            allAncestors.add(0, parentContext);
            parentContext = parentContext.parentContext();
        }
        for (LockContext l : allAncestors) {
            LockType parentLock = l.getExplicitLockType(transaction);
            if (requestType.equals(LockType.S)) {
                //request type is S
                if (parentLock.equals(LockType.NL)) {
                    l.acquire(transaction, LockType.IS);
                }
            } else {
                //request type is X
                switch (parentLock) {
                    case NL: l.acquire(transaction, LockType.IX);
                        break;
                    case IS: l.promote(transaction, LockType.IX);
                        break;
                    case S: l.promote(transaction, LockType.SIX);
                        break;
                    default: break;
                }
            }
        }
        return;
    }
}
