package com.weikai77.fdb.util.concurrent;

import org.junit.Assert;
import org.junit.Test;

import com.weikai77.util.SettableClock;

/**
 * 
 * @author kwei
 *
 */
public class DistributedSimpleLockTestIT
{
  @Test
  public void testSingleThreaded() throws Exception
  {
    SettableClock clock = new SettableClock(System.currentTimeMillis());
    DistributedLockMgr lockMgr = TestUtils.getLockMgr(clock);
    DistributedSimpleLock lock1 = lockMgr.newSimpleLock("myTestLock");
    DistributedSimpleLock lock2 = lockMgr.newSimpleLock("myTestLock");
    
    try
    {
      Assert.assertFalse(lock1.hasLock());
      Assert.assertTrue(lock1.getInitialLockTime() <= 0);
      Assert.assertTrue(lock1.getLastLockTime() <= 0);

      // lock1 locked
      long now = clock.currentTimeMillis();
      lock1.acquire();
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(now, lock1.getInitialLockTime());
      Assert.assertEquals(now, lock1.getLastLockTime());

      // lock1 unlocked
      lock1.release();
      Assert.assertFalse(lock1.hasLock());
      Assert.assertTrue(lock1.getInitialLockTime() <= 0);
      Assert.assertTrue(lock1.getLastLockTime() <= 0);

      // lock1 locked
      clock.tick(100);
      now = clock.currentTimeMillis();
      lock1.acquire();
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(now, lock1.getInitialLockTime());
      Assert.assertEquals(now, lock1.getLastLockTime());

      // lock1 locked #2
      clock.tick(100);
      now = clock.currentTimeMillis();
      lock1.acquire();
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(now-100, lock1.getInitialLockTime());
      Assert.assertEquals(now, lock1.getLastLockTime());

      // lock1 locked #3
      clock.tick(200);
      now = clock.currentTimeMillis();
      lock1.acquire();
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(now-300, lock1.getInitialLockTime());
      Assert.assertEquals(now, lock1.getLastLockTime());

      // lock2 can't lock
      Assert.assertFalse(lock2.tryAcquire());
      Assert.assertFalse(lock2.tryAcquire(100, 60_000));
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(now-300, lock1.getInitialLockTime());
      Assert.assertEquals(now, lock1.getLastLockTime());
      Assert.assertFalse(lock2.hasLock());
      Assert.assertTrue(lock2.getInitialLockTime() <= 0);
      Assert.assertTrue(lock2.getLastLockTime() <= 0);

      // lock1 unlocked
      lock1.release();
      Assert.assertFalse(lock1.hasLock());
      Assert.assertTrue(lock1.getInitialLockTime() <= 0);
      Assert.assertTrue(lock1.getLastLockTime() <= 0);

      // unlock should fail and have no effect
      try
      {
        lock1.release();
        Assert.fail("Should have failed but did not");
      }
      catch (IllegalMonitorStateException ex) {}
      Assert.assertFalse(lock1.hasLock());
      Assert.assertTrue(lock1.getInitialLockTime() <= 0);
      Assert.assertTrue(lock1.getLastLockTime() <= 0);

      // lock2 can lock now
      clock.tick(150);
      now = clock.currentTimeMillis();
      Assert.assertTrue(lock2.tryAcquire());
      Assert.assertTrue(lock2.hasLock());
      Assert.assertEquals(now, lock2.getInitialLockTime());
      Assert.assertEquals(now, lock2.getLastLockTime());
      Assert.assertFalse(lock1.hasLock());
      Assert.assertTrue(lock1.getInitialLockTime() <= 0);
      Assert.assertTrue(lock1.getLastLockTime() <= 0);

      // lock2 unlocked
      lock2.release();
      Assert.assertFalse(lock2.hasLock());
      Assert.assertTrue(lock2.getInitialLockTime() <= 0);
      Assert.assertTrue(lock2.getLastLockTime() <= 0);

      // lock2 locked
      clock.tick(150);
      now = clock.currentTimeMillis();
      Assert.assertTrue(lock2.tryAcquire(1000, 60_000));
      Assert.assertTrue(lock2.hasLock());
      Assert.assertEquals(now, lock2.getInitialLockTime());
      Assert.assertEquals(now, lock2.getLastLockTime());

      // lock2 expired
      clock.tick(60_001);
      Assert.assertFalse(lock2.hasLock());
      Assert.assertTrue(lock2.getInitialLockTime() <= 0);
      Assert.assertTrue(lock2.getLastLockTime() <= 0);

      // lock2 locked
      now = clock.currentTimeMillis();
      Assert.assertTrue(lock2.tryAcquire());
      Assert.assertTrue(lock2.hasLock());
      Assert.assertEquals(now, lock2.getInitialLockTime());
      Assert.assertEquals(now, lock2.getLastLockTime());

      // lock2 locked #2 (with reduced TTL)
      now = clock.currentTimeMillis();
      lock2.acquire(30_000);
      Assert.assertTrue(lock2.hasLock());
      Assert.assertEquals(now, lock2.getInitialLockTime());
      Assert.assertEquals(now, lock2.getLastLockTime());
      
      // lock2 expired
      clock.tick(45_000);
      Assert.assertFalse(lock2.hasLock());
      Assert.assertTrue(lock2.getInitialLockTime() <= 0);
      Assert.assertTrue(lock2.getLastLockTime() <= 0);

      // lock1 can lock now
      now = clock.currentTimeMillis();
      lock1.acquire();
      Assert.assertTrue(lock1.hasLock());
      Assert.assertEquals(now, lock1.getInitialLockTime());
      Assert.assertEquals(now, lock1.getLastLockTime());
    }
    finally
    {
      lockMgr.deleteSimpleLock("myTestLock");
      Assert.assertFalse(lock1.hasLock());
      Assert.assertFalse(lock2.hasLock());
    }
  }
  
  @Test
  public void testMultiThreaded() throws Exception
  {
    DistributedLockMgr lockMgr = TestUtils.getLockMgr();
    DistributedSimpleLock lock2 = lockMgr.newSimpleLock("myTestLock");
    Assert.assertFalse(lock2.hasLock());
    DistributedSimpleLock lock1 = lockMgr.newSimpleLock("myTestLock");
    Assert.assertFalse(lock1.hasLock());

    try
    {
      // lock1 locked
      lock1.acquire();
      Assert.assertTrue(lock1.hasLock());
      Assert.assertFalse(lock2.hasLock());

      // lock2 can't lock
      Assert.assertFalse(lock2.tryAcquire());
      long start = System.currentTimeMillis();
      Assert.assertFalse(lock2.tryAcquire(200, 0));
      Assert.assertTrue(System.currentTimeMillis() - start >= 200);
      Assert.assertFalse(lock2.hasLock());
      
      // lock1 locked with TTL
      lock1.acquire(100);

      // lock2 can lock after lock1 expires
      Thread.sleep(150);
      lock2.acquire();
      Assert.assertFalse(lock1.hasLock());
      Assert.assertTrue(lock2.hasLock());
      
      Thread t1 = new Thread(new Runnable()
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
      });
      t1.start();

      // lock1 can't lock
      Assert.assertFalse(lock1.tryAcquire(100, 0));
      Assert.assertTrue(lock2.hasLock());
      Assert.assertFalse(lock1.hasLock());

      // lock1 can lock now
      Assert.assertTrue(lock1.tryAcquire(500));
      Assert.assertTrue(lock1.hasLock());
      Assert.assertFalse(lock2.hasLock());

      // lock1 locked #2 (with TTL)
      Assert.assertTrue(lock1.tryAcquire(100, 200));

      // lock2 can't lock
      Assert.assertFalse(lock2.tryAcquire());
      Assert.assertTrue(lock1.hasLock());
      Assert.assertFalse(lock2.hasLock());
      
      // lock2 can lock after lock1 expires
      Thread.sleep(300);
      Assert.assertTrue(lock2.tryAcquire());
      Assert.assertFalse(lock1.hasLock());
      Assert.assertTrue(lock2.hasLock());
      
      t1.join();
    }
    finally
    {
      lockMgr.deleteSimpleLock("myTestLock");
      Assert.assertFalse(lock1.hasLock());
      Assert.assertFalse(lock2.hasLock());
    }
  }
}
