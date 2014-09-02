package com.weikai77.fdb.util.concurrent;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.weikai77.fdb.util.DirectoryBasedSpace;
import com.weikai77.fdb.util.FdbUtils;

/**
 * 
 * @author kwei
 *
 */
public class DistributedConditionTestIT
{
  @Test
  public void testSingleThreaded() throws Exception
  {
    DistributedLockMgr lockMgr = TestUtils.getLockMgr();
    DistributedCondition cond = lockMgr.newCondition("myTestCondition");

    try
    {
      Assert.assertFalse(cond.await(100, TimeUnit.MILLISECONDS));
      cond.signalAll();
      Assert.assertFalse(cond.await(100, TimeUnit.MILLISECONDS));
    }
    finally
    {
      lockMgr.deleteCondition("myTestCondition");
    }
  }

  @Test
  public void testMultiThreaded() throws Exception
  {
    DistributedLockMgr lockMgr = TestUtils.getLockMgr();
    DistributedCondition cond1 = lockMgr.newCondition("myTestCondition");
    DistributedCondition cond2 = lockMgr.newCondition("myTestCondition");

    try
    {
      Assert.assertFalse(cond1.await(100, TimeUnit.MILLISECONDS));
  
      new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          try
          {
            Thread.sleep(100);
            cond2.signalAll();
          }
          catch (InterruptedException ex) {}
        }
      }).start();
  
      Assert.assertTrue(cond1.await(500, TimeUnit.MILLISECONDS));
      Assert.assertFalse(cond1.await(100, TimeUnit.MILLISECONDS));
      Assert.assertFalse(cond2.await(100, TimeUnit.MILLISECONDS));
  
      new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          try
          {
            Thread.sleep(100);
            cond1.signalAll();
          }
          catch (InterruptedException ex) {}
        }
      }).start();
  
      cond1.await();
      Assert.assertFalse(cond2.await(100, TimeUnit.MILLISECONDS));
    }
    finally
    {
      lockMgr.deleteCondition("myTestCondition");
    }
  }
  
  @Test
  public void testWithLock() throws Exception
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();

    DistributedLockMgr lockMgr = TestUtils.getLockMgr();
    DistributedSimpleLock lock = lockMgr.newSimpleLock("myTestLockWithConditions");
    
    try
    {
      DistributedCondition cond = lock.newCondition("myTestCondition");
      byte[] myTestKey = "myTestKey".getBytes();
  
      Assert.assertFalse(lock.hasLock());
      try
      {
        cond.await();
        Assert.fail("Expected to fail but did not!");
      }
      catch (IllegalMonitorStateException ex) {}
      try
      {
        cond.signalAll();
        Assert.fail("Expected to fail but did not!");
      }
      catch (IllegalMonitorStateException ex) {}
      Assert.assertFalse(lock.hasLock());
  
      Thread t = null;
      lock.acquire();
      try
      {
        Assert.assertTrue(lock.hasLock());
        Assert.assertFalse(cond.await(100, TimeUnit.MILLISECONDS));
        Assert.assertTrue(lock.hasLock());
    
        cond.signalAll();
        Assert.assertTrue(lock.hasLock());
        Assert.assertFalse(cond.await(100, TimeUnit.MILLISECONDS));
        Assert.assertTrue(lock.hasLock());
        
        t = new Thread(new Runnable()
        {
          public void run()
          {
            DistributedSimpleLock lock = lockMgr.newSimpleLock("myTestLockWithConditions");
            DistributedCondition cond = lock.newCondition("myTestCondition");
  
            lock.acquire();
            try
            {
              FdbUtils.set(db, myTestKey, "abc".getBytes());
              cond.signalAll();
            }
            finally
            {
              lock.release();
              Assert.assertFalse(lock.hasLock());
            }
  
            try
            {
              Thread.sleep(500);
            }
            catch (InterruptedException ex) {}
  
            lock.acquire();
            try
            {
              FdbUtils.set(db, myTestKey, "def".getBytes());
              cond.signalAll();
            }
            finally
            {
              lock.release();
              Assert.assertFalse(lock.hasLock());
            }
          }
        });
        
        t.start();
  
        byte[] value = null;
        while (!Arrays.equals(value = FdbUtils.get(db, myTestKey), 
                              "def".getBytes()))
        {
          System.out.println("myTestKey => " + (value == null ? null : new String(value)));
          cond.await();
        }
      }
      finally
      {
        t.join();
        FdbUtils.clear(db, myTestKey);
      }
    }
    finally
    {
      lockMgr.deleteSimpleLock("myTestLockWithConditions");
      Assert.assertFalse(lock.hasLock());
    }
  }
  
  //@Test disabled before interruption does not work today
  public void testInterruptionTupleBased() throws Exception
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();
    
    testInterruption(db, TestUtils.getLockMgr());
  }
  
  //@Test disabled before interruption does not work today
  public void testInterruptionDirectoryBased() throws Exception
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();
    
    testInterruption(db, new DistributedLockMgr(db, new DirectoryBasedSpace(db, "test")));
  }
  
  private void testInterruption(Database db, DistributedLockMgr lockMgr) throws Exception
  {
    Thread t = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        DistributedCondition cond = lockMgr.newCondition("myTestCondition");
        
        try
        {
          cond.await();
          Assert.fail("Should have thrown InterruptedException but did not");
        }
        catch (InterruptedException ex) {}
      }
    });
    
    t.start();
    t.interrupt();
    t.join();
  }
}
