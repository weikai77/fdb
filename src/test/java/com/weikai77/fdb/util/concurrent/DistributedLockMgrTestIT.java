package com.weikai77.fdb.util.concurrent;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author kwei
 *
 */
public class DistributedLockMgrTestIT
{
  @Test
  public void testCRUD() throws Exception
  {
    DistributedLockMgr mgr = TestUtils.getLockMgr();

    // conditions
    Assert.assertEquals(0, mgr.listConditions().size());
    mgr.newCondition("myTestCondition");
    Assert.assertEquals(1, mgr.listConditions().size());
    mgr.deleteCondition("myTestCondition");
    Assert.assertEquals(0, mgr.listConditions().size());
    mgr.newCondition("myTestCondition");
    Assert.assertEquals(1, mgr.listConditions().size());
    mgr.newCondition("myTestCondition");
    Assert.assertEquals(1, mgr.listConditions().size());
    mgr.newCondition("myTestCondition2");
    Assert.assertEquals(2, mgr.listConditions().size());
    mgr.deleteCondition("myTestCondition");
    Assert.assertEquals(1, mgr.listConditions().size());
    mgr.deleteCondition("myTestCondition2");
    Assert.assertEquals(0, mgr.listConditions().size());

    // simple locks
    Assert.assertEquals(0, mgr.listSimpleLocks().size());
    mgr.newSimpleLock("myTestLock");
    Assert.assertEquals(1, mgr.listSimpleLocks().size());
    mgr.deleteSimpleLock("myTestLock");
    Assert.assertEquals(0, mgr.listSimpleLocks().size());
    mgr.newSimpleLock("myTestLock");
    Assert.assertEquals(1, mgr.listSimpleLocks().size());
    mgr.newSimpleLock("myTestLock");
    Assert.assertEquals(1, mgr.listSimpleLocks().size());
    mgr.newSimpleLock("myTestLock2");
    Assert.assertEquals(2, mgr.listSimpleLocks().size());
    mgr.deleteSimpleLock("myTestLock");
    Assert.assertEquals(1, mgr.listSimpleLocks().size());
    mgr.deleteSimpleLock("myTestLock2");
    Assert.assertEquals(0, mgr.listSimpleLocks().size());

    // reentrant locks
    Assert.assertEquals(0, mgr.listReentrantLocks().size());
    mgr.newReentrantLock("myTestReentrantLock");
    Assert.assertEquals(1, mgr.listReentrantLocks().size());
    mgr.deleteReentrantLock("myTestReentrantLock");
    Assert.assertEquals(0, mgr.listReentrantLocks().size());
    mgr.newReentrantLock("myTestReentrantLock");
    Assert.assertEquals(1, mgr.listReentrantLocks().size());
    mgr.newReentrantLock("myTestReentrantLock");
    Assert.assertEquals(1, mgr.listReentrantLocks().size());
    mgr.newReentrantLock("myTestReentrantLock2");
    Assert.assertEquals(2, mgr.listReentrantLocks().size());
    mgr.deleteReentrantLock("myTestReentrantLock");
    Assert.assertEquals(1, mgr.listReentrantLocks().size());
    mgr.deleteReentrantLock("myTestReentrantLock2");
    Assert.assertEquals(0, mgr.listReentrantLocks().size());

    // read/write locks
    Assert.assertEquals(0, mgr.listReadWriteLocks().size());
    mgr.newReadWriteLock("myTestReadWriteLock");
    Assert.assertEquals(1, mgr.listReadWriteLocks().size());
    mgr.deleteReadWriteLock("myTestReadWriteLock");
    Assert.assertEquals(0, mgr.listReadWriteLocks().size());
    mgr.newReadWriteLock("myTestReadWriteLock");
    Assert.assertEquals(1, mgr.listReadWriteLocks().size());
    mgr.newReadWriteLock("myTestReadWriteLock");
    Assert.assertEquals(1, mgr.listReadWriteLocks().size());
    mgr.newReadWriteLock("myTestReadWriteLock2");
    Assert.assertEquals(2, mgr.listReadWriteLocks().size());
    mgr.deleteReadWriteLock("myTestReadWriteLock");
    Assert.assertEquals(1, mgr.listReadWriteLocks().size());
    mgr.deleteReadWriteLock("myTestReadWriteLock2");
    Assert.assertEquals(0, mgr.listReadWriteLocks().size());

    // count down latches
    Assert.assertEquals(0, mgr.listCoundDownLatches().size());
    mgr.newCountDownLatch("myTestCountDownLatch");
    Assert.assertEquals(1, mgr.listCoundDownLatches().size());
    mgr.deleteCountDownLatch("myTestCountDownLatch");
    Assert.assertEquals(0, mgr.listCoundDownLatches().size());
    mgr.newCountDownLatch("myTestCountDownLatch");
    Assert.assertEquals(1, mgr.listCoundDownLatches().size());
    mgr.newCountDownLatch("myTestCountDownLatch");
    Assert.assertEquals(1, mgr.listCoundDownLatches().size());
    mgr.newCountDownLatch("myTestCountDownLatch2");
    Assert.assertEquals(2, mgr.listCoundDownLatches().size());
    mgr.deleteCountDownLatch("myTestCountDownLatch");
    Assert.assertEquals(1, mgr.listCoundDownLatches().size());
    mgr.deleteCountDownLatch("myTestCountDownLatch2");
    Assert.assertEquals(0, mgr.listCoundDownLatches().size());
  }
}
