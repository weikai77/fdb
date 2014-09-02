package com.weikai77.fdb.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author kwei
 *
 */
public class DistributedCountDownLatchTestIT
{
  @Test
  public void testSingleThreaded() throws Exception
  {
    DistributedLockMgr lockMgr = TestUtils.getLockMgr();
    DistributedCountDownLatch latch = lockMgr.newCountDownLatch("myTestCountDownLatch");
    
    try
    {
      Assert.assertEquals(0, latch.getCount());
      Assert.assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
      latch.await();
      
      latch.setCount(10);
      for (int i=0; i<10; i++)
      {
        Assert.assertEquals(10-i, latch.getCount());
        Assert.assertFalse(latch.await(100, TimeUnit.MILLISECONDS));
        latch.countDown();
      }

      latch.countDown();
      Assert.assertEquals(0, latch.getCount());
      Assert.assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
      latch.await();

      latch.countDown();
      Assert.assertEquals(0, latch.getCount());
      Assert.assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
      latch.await();
    }
    finally
    {
      lockMgr.deleteCountDownLatch("myTestCountDownLatch");
    }
  }
  
  @Test
  public void testMultiThreaded() throws Exception
  {
    DistributedLockMgr lockMgr = TestUtils.getLockMgr();
    DistributedCountDownLatch latch = lockMgr.newCountDownLatch("myTestCountDownLatch");
    
    try
    {
      ExecutorService executor = Executors.newFixedThreadPool(20);
      List<Future<?>> futures = new ArrayList<>();
      latch.setCount(10);
      
      for (int i=0; i<10; i++)
      {
        futures.add(executor.submit(new Runnable()
        {
          @Override
          public void run()
          {
            latch.await();
          }
        }));

        futures.add(executor.submit(new Runnable()
        {
          @Override
          public void run()
          {
            latch.countDown();
          }
        }));
      }
      
      latch.await();

      for (Future<?> future : futures)
      {
        future.get();
      }
    }
    finally
    {
      lockMgr.deleteCountDownLatch("myTestCountDownLatch");
    }
  }
}
