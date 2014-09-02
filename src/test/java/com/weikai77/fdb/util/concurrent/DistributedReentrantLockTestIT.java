package com.weikai77.fdb.util.concurrent;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.weikai77.fdb.util.concurrent.DistributedLockMgr;
import com.weikai77.fdb.util.concurrent.DistributedReentrantLock;

/**
 * 
 * @author kwei
 *
 */
public class DistributedReentrantLockTestIT
{
  @Test
  public void testSingleThreaded() throws Exception
  {
    DistributedLockMgr lockMgr = TestUtils.getLockMgr();
    DistributedReentrantLock lock1 = lockMgr.newReentrantLock("myTestLock");
    DistributedReentrantLock lock2 = lockMgr.newReentrantLock("myTestLock");
    
    try
    {
      Assert.assertFalse(lock1.hasLock());
      Assert.assertEquals(0, lock1.getHoldCount());
      Assert.assertEquals(0, lock1.getLastAcquired());

      // lock1 locked
      long now = System.currentTimeMillis();
      lock1.acquire();
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(1, lock1.getHoldCount());
      Assert.assertTrue(now <= lock1.getLastAcquired());

      // lock1 hold count to 2
      Thread.sleep(100);
      now = System.currentTimeMillis();
      lock1.acquire();
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(2, lock1.getHoldCount());
      Assert.assertTrue(now <= lock1.getLastAcquired());

      // lock1 hold count to 1
      lock1.release();
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(1, lock1.getHoldCount());
      Assert.assertTrue(now <= lock1.getLastAcquired());

      // lock2 can't lock
      Assert.assertFalse(lock2.tryAcquire());
      Assert.assertFalse(lock2.tryAcquire(100, TimeUnit.MILLISECONDS));
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(1, lock1.getHoldCount());
      Assert.assertTrue(now <= lock1.getLastAcquired());
      Assert.assertFalse(lock2.hasLock());
      Assert.assertEquals(0, lock2.getHoldCount());
      Assert.assertEquals(0, lock2.getLastAcquired());

      // lock1 hold count to 2
      Thread.sleep(100);
      now = System.currentTimeMillis();
      Assert.assertTrue(lock1.tryAcquire());
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(2, lock1.getHoldCount());
      Assert.assertTrue(now <= lock1.getLastAcquired());

      // lock1 hold count to 1
      lock1.release();
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(1, lock1.getHoldCount());
      Assert.assertTrue(now <= lock1.getLastAcquired());

      // lock1 hold count to 2
      now = System.currentTimeMillis();
      Assert.assertTrue(lock1.tryAcquire(1000, TimeUnit.MILLISECONDS));
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(2, lock1.getHoldCount());
      Assert.assertTrue(now <= lock1.getLastAcquired());

      // lock1 hold count to 1
      lock1.release();
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(1, lock1.getHoldCount());
      Assert.assertTrue(now <= lock1.getLastAcquired());

      // lock1 unlocked
      lock1.release();
      Assert.assertFalse(lock1.hasLock());
      Assert.assertEquals(0, lock1.getHoldCount());
      Assert.assertEquals(0, lock1.getLastAcquired());

      // unlock should fail and have no effect
      try
      {
        lock1.release();
        Assert.fail("Should have failed but did not");
      }
      catch (IllegalMonitorStateException ex) {}
      Assert.assertFalse(lock1.hasLock());
      Assert.assertEquals(0, lock1.getHoldCount());
      Assert.assertEquals(0, lock1.getLastAcquired());

      // lock2 can lock now
      now = System.currentTimeMillis();
      Assert.assertTrue(lock2.tryAcquire());
      Assert.assertTrue(lock2.hasLock());
      Assert.assertEquals(1, lock2.getHoldCount());
      Assert.assertTrue(now <= lock2.getLastAcquired());
      Assert.assertFalse(lock1.hasLock());
      Assert.assertEquals(0, lock1.getHoldCount());
      Assert.assertEquals(0, lock1.getLastAcquired());

      // lock2 unlocked
      lock2.release();
      Assert.assertFalse(lock2.hasLock());
      Assert.assertEquals(0, lock2.getHoldCount());
      Assert.assertEquals(0, lock2.getLastAcquired());

      now = System.currentTimeMillis();
      Assert.assertTrue(lock2.tryAcquire(1000, TimeUnit.MILLISECONDS));
      Assert.assertTrue(lock2.hasLock());
      Assert.assertEquals(1, lock2.getHoldCount());
      Assert.assertTrue(now <= lock2.getLastAcquired());
    }
    finally
    {
      lockMgr.deleteReentrantLock("myTestLock");;
      Assert.assertFalse(lock1.hasLock());
      Assert.assertEquals(0, lock1.getHoldCount());
      Assert.assertFalse(lock2.hasLock());
      Assert.assertEquals(0, lock2.getHoldCount());
    }
  }
  
  @Test
  public void testMultiThreaded() throws Exception
  {
    DistributedLockMgr lockMgr = TestUtils.getLockMgr();
    DistributedReentrantLock lock1 = lockMgr.newReentrantLock("myTestLock");
    Assert.assertFalse(lock1.hasLock());
    DistributedReentrantLock lock2 = lockMgr.newReentrantLock("myTestLock");
    Assert.assertFalse(lock2.hasLock());

    try
    {
      lock1.acquire();
      Assert.assertTrue(lock1.hasLock());
      Assert.assertFalse(lock2.hasLock());
      
      Assert.assertFalse(lock2.tryAcquire());
      long start = System.currentTimeMillis();
      Assert.assertFalse(lock2.tryAcquire(500, TimeUnit.MILLISECONDS));
      Assert.assertTrue(System.currentTimeMillis() - start >= 500);
      Assert.assertFalse(lock2.hasLock());

      lock1.release();
      Assert.assertFalse(lock1.hasLock());
      Assert.assertTrue(lock2.tryAcquire());
      Assert.assertTrue(lock2.hasLock());
      
      new Thread(new Runnable()
      {
        public void run()
        {
          try
          {
            Thread.sleep(200);
            lock2.release();
          }
          catch (InterruptedException ex) {}
        }
      }).start();
      
      Assert.assertFalse(lock1.tryAcquire(100, TimeUnit.MILLISECONDS));
      Assert.assertTrue(lock2.hasLock());
      Assert.assertFalse(lock1.hasLock());

      Assert.assertTrue(lock1.tryAcquire(500, TimeUnit.MILLISECONDS));
      Assert.assertTrue(lock1.hasLock());
      Assert.assertFalse(lock2.hasLock());
      
      lock1.acquire();
      Assert.assertEquals(2, lock1.getHoldCount());

      new Thread(new Runnable()
      {
        public void run()
        {
          try
          {
            Thread.sleep(100);
            lock1.release();

            Thread.sleep(200);
            lock1.release();
          }
          catch (InterruptedException ex) {}
        }
      }).start();
      
      Assert.assertFalse(lock2.tryAcquire(200, TimeUnit.MILLISECONDS));
      Assert.assertTrue(lock1.hasLock());
      Assert.assertFalse(lock2.hasLock());
      Assert.assertTrue(lock2.tryAcquire(500, TimeUnit.MILLISECONDS));
      Assert.assertFalse(lock1.hasLock());
      Assert.assertTrue(lock2.hasLock());
    }
    finally
    {
      lockMgr.deleteReentrantLock("myTestLock");;
      Assert.assertFalse(lock1.hasLock());
      Assert.assertFalse(lock2.hasLock());
    }
  }
}
