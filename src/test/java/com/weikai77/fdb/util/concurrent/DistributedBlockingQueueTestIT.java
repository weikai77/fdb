package com.weikai77.fdb.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
public class DistributedBlockingQueueTestIT
{
  @Test
  public void testSingleThreadedTupleBased() throws Exception
  {
    testSingleThreaded(TestUtils.getTupleBasedQueueMgr(), false);
  }
  
  @Test
  public void testSingleThreadedTupleBasedWithLocking() throws Exception
  {
    testSingleThreaded(TestUtils.getTupleBasedQueueMgr(), true);
  }
  
  @Test
  public void testSingleThreadedDirectoryBased() throws Exception
  {
    testSingleThreaded(TestUtils.getDirectoryBasedQueueMgr(), false);
  }
  
  @Test
  public void testSingleThreadedDirectoryBasedWithLocking() throws Exception
  {
    testSingleThreaded(TestUtils.getDirectoryBasedQueueMgr(), true);
  }
  
  private void testSingleThreaded(DistributedQueueMgr queueMgr, boolean withLocking) throws Exception
  {
    // unbounded queue
    queueMgr.createBlockingQueue("myTestQueue");
    try
    {
      BlockingQueue queue = withLocking ? queueMgr.getBlockingQueueWithLocking("myTestQueue") : 
        queueMgr.getBlockingQueue("myTestQueue");
      assertQueueEmpty(queue);
      
      byte[] foo = "foo".getBytes();
      queue.offer(foo);
      Assert.assertEquals(1l, queue.size());
      Assert.assertArrayEquals(foo, queue.poll());
      assertQueueEmpty(queue);
  
      Random r = new Random();
      List<byte[]> items = new ArrayList<byte[]>();
      for (int i=0; i<50; i++)
      {
        byte[] bytes = new byte[10];
        r.nextBytes(bytes);
        items.add(bytes);
      }
  
      int size = 0;
      for (byte[] item : items)
      {
        int dice = r.nextInt(3);
        if (dice == 0)
        {
          queue.offer(item);
        }
        else if (dice == 1)
        {
          queue.put(item);
        }
        else
        {
          queue.put(item, 100, TimeUnit.MILLISECONDS);
        }
        Assert.assertEquals(++size, queue.size());
      }
      
      for (byte[] item : items)
      {
        Assert.assertEquals(size--, queue.size());
        int dice = r.nextInt(3);
        if (dice == 0)
        {
          Assert.assertArrayEquals(item, queue.poll());
        }
        else if (dice == 1)
        {
          Assert.assertArrayEquals(item, queue.take());
        }
        else
        {
          Assert.assertArrayEquals(item, queue.take(100, TimeUnit.MILLISECONDS));
        }
      }

      assertQueueEmpty(queue);
    }
    finally
    {
      queueMgr.deleteQueue("myTestQueue");
    }

    // bounded queue of capacity 10
    queueMgr.createBlockingQueue("myTestQueue", 10);
    try
    {
      BlockingQueue queue = withLocking ? queueMgr.getBlockingQueueWithLocking("myTestQueue") :
        queueMgr.getBlockingQueue("myTestQueue");
      assertQueueEmpty(queue);

      byte[] foo = "foo".getBytes();
      queue.offer(foo);
      Assert.assertEquals(1l, queue.size());
      Assert.assertArrayEquals(foo, queue.poll());
      assertQueueEmpty(queue);

      Random r = new Random();
      List<byte[]> items = new ArrayList<byte[]>();
      for (int i=0; i<10; i++)
      {
        byte[] bytes = new byte[10];
        r.nextBytes(bytes);
        items.add(bytes);
      }

      int size = 0;
      for (byte[] item : items)
      {
        int dice = r.nextInt(3);
        if (dice == 0)
        {
          Assert.assertTrue(queue.offer(item));
        }
        else if (dice == 1)
        {
          queue.put(item);
        }
        else
        {
          Assert.assertTrue(queue.put(item, 100, TimeUnit.MILLISECONDS));
        }
        Assert.assertEquals(++size, queue.size());
      }

      assertQueueFull(queue);

      for (byte[] item : items)
      {
        Assert.assertEquals(size--, queue.size());
        int dice = r.nextInt(3);
        if (dice == 0)
        {
          Assert.assertArrayEquals(item, queue.poll());
        }
        else if (dice == 1)
        {
          Assert.assertArrayEquals(item, queue.take());
        }
        else
        {
          Assert.assertArrayEquals(item, queue.take(100, TimeUnit.MILLISECONDS));
        }
      }

      assertQueueEmpty(queue);
    }
    finally
    {
      queueMgr.deleteQueue("myTestQueue");
    }
  }
  
  private void assertQueueEmpty(BlockingQueue queue) throws Exception
  {
    Assert.assertEquals(0l, queue.size());
    Assert.assertNull(queue.poll());
    long start = System.currentTimeMillis();
    Assert.assertNull(queue.take(100, TimeUnit.MILLISECONDS));
    Assert.assertTrue(System.currentTimeMillis() - start >= 100);
  }
  
  private void assertQueueFull(BlockingQueue queue) throws Exception
  {
    byte[] foo = "foo".getBytes();
    Assert.assertTrue(queue.size() >= queue.capacity());
    Assert.assertFalse(queue.offer(foo));
    long start = System.currentTimeMillis();
    Assert.assertFalse(queue.put(foo, 100, TimeUnit.MILLISECONDS));
    Assert.assertTrue(System.currentTimeMillis() - start >= 100);
  }
  
  @Test
  public void testMultiThreadedTupleBased() throws Exception
  {
    testMultiThreaded(TestUtils.getTupleBasedQueueMgr(), false);
  }

  @Test
  public void testMultiThreadedTupleBasedWithLocking() throws Exception
  {
    testMultiThreaded(TestUtils.getTupleBasedQueueMgr(), true);
  }

  @Test
  public void testMultiThreadedDirectoryBased() throws Exception
  {
    testMultiThreaded(TestUtils.getDirectoryBasedQueueMgr(), false);
  }
  
  @Test
  public void testMultiThreadedDirectoryBasedWithLocking() throws Exception
  {
    testMultiThreaded(TestUtils.getDirectoryBasedQueueMgr(), true);
  }
  
  private void testMultiThreaded(DistributedQueueMgr queueMgr, boolean withLocking) throws Exception
  {
    queueMgr.createBlockingQueue("myTestQueue");
    try
    {
      doTestMultiThreaded(queueMgr, withLocking);
    }
    finally
    {
      queueMgr.deleteQueue("myTestQueue");
    }

    queueMgr.createBlockingQueue("myTestQueue", 10);
    try
    {
      doTestMultiThreaded(queueMgr, withLocking);
    }
    finally
    {
      queueMgr.deleteQueue("myTestQueue");
    }
  }

  /**
   * 1 producer + 1 consumer, verify the items are retrieved in order
   */
  private void doTestMultiThreaded(DistributedQueueMgr queueMgr, boolean withLocking) throws Exception
  {
    Random r = new Random();
    List<byte[]> items = new ArrayList<byte[]>();
    for (int i=0; i<50; i++)
    {
      byte[] bytes = new byte[10];
      r.nextBytes(bytes);
      items.add(bytes);
    }

    BlockingQueue queue = withLocking ? queueMgr.getBlockingQueueWithLocking("myTestQueue") :
      queueMgr.getBlockingQueue("myTestQueue");
    assertQueueEmpty(queue);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<?> f1 = executor.submit(new Runnable()
    {
      public void run()
      {
        try
        {
          BlockingQueue queue = withLocking ? queueMgr.getBlockingQueueWithLocking("myTestQueue") :
            queueMgr.getBlockingQueue("myTestQueue");
          for (byte[] item : items)
          {
            int dice = r.nextInt(3);
            if (dice == 0)
            {
              while (!queue.offer(item)) {}
            }
            else if (dice == 1)
            {
              queue.put(item);
            }
            else
            {
              while (!queue.put(item, 100, TimeUnit.MILLISECONDS)) {}
            }
  
            Thread.sleep(r.nextInt(10));
            //System.out.println("[producer] queue size = " + queue.size());
          }
        }
        catch (InterruptedException ex)
        {
          throw new RuntimeException(ex);
        }
      }
    });

    Future<?> f2 = executor.submit(new Runnable()
    {
      public void run()
      {
        try
        {
          BlockingQueue queue = withLocking ? queueMgr.getBlockingQueueWithLocking("myTestQueue") :
            queueMgr.getBlockingQueue("myTestQueue");
          for (byte[] item : items)
          {
            byte[] taken = null;
            int dice = r.nextInt(3);
            if (dice == 0)
            {
              while ((taken = queue.poll()) == null) {}
            }
            else if (dice == 1)
            {
              taken = queue.take();
            }
            else
            {
              while ((taken = queue.take(100, TimeUnit.MILLISECONDS)) == null) {}
            }
            Assert.assertArrayEquals(item, taken);
            Thread.sleep(r.nextInt(10));
            //System.out.println("[consumer] queue size = " + queue.size());
          }
        }
        catch (InterruptedException ex)
        {
          throw new RuntimeException(ex);
        }
      }
    });

    f1.get();
    f2.get();

    assertQueueEmpty(queue);
  }

  @Test
  public void testMultiThreaded2TupleBased() throws Exception
  {
    testMultiThreaded2(TestUtils.getTupleBasedQueueMgr(), false);
  }

  @Test
  public void testMultiThreaded2TupleBasedWithLocking() throws Exception
  {
    testMultiThreaded2(TestUtils.getTupleBasedQueueMgr(), true);
  }

  @Test
  public void testMultiThreaded2DirectoryBased() throws Exception
  {
    testMultiThreaded2(TestUtils.getDirectoryBasedQueueMgr(), false);
  }
  
  @Test
  public void testMultiThreaded2DirectoryBasedWithLocking() throws Exception
  {
    testMultiThreaded2(TestUtils.getDirectoryBasedQueueMgr(), true);
  }
  
  private void testMultiThreaded2(DistributedQueueMgr queueMgr, boolean withLocking) throws Exception
  {
    queueMgr.createBlockingQueue("myTestQueue");
    try
    {
      doTestMultiThreaded2(queueMgr, withLocking);
    }
    finally
    {
      queueMgr.deleteQueue("myTestQueue");
    }

    queueMgr.createBlockingQueue("myTestQueue", 10);
    try
    {
      doTestMultiThreaded2(queueMgr, withLocking);
    }
    finally
    {
      queueMgr.deleteQueue("myTestQueue");
    }
  }

  /**
   * 2 producers + 2 consumers, verify the same number of items are enqueued and dequeued
   */
  private void doTestMultiThreaded2(DistributedQueueMgr queueMgr, boolean withLocking) throws Exception
  {
    Random r = new Random();
    List<byte[]> items = new ArrayList<byte[]>();
    for (int i=0; i<50; i++)
    {
      byte[] bytes = new byte[10];
      r.nextBytes(bytes);
      items.add(bytes);
    }

    BlockingQueue queue = withLocking ? queueMgr.getBlockingQueueWithLocking("myTestQueue") :
      queueMgr.getBlockingQueue("myTestQueue");
    assertQueueEmpty(queue);

    ExecutorService executor = Executors.newFixedThreadPool(4);
    Future<?> f1 = executor.submit(new Runnable()
    {
      public void run()
      {
        try
        {
          BlockingQueue queue = withLocking ? queueMgr.getBlockingQueueWithLocking("myTestQueue") :
            queueMgr.getBlockingQueue("myTestQueue");
          
          for (byte[] item : items)
          {
            int dice = r.nextInt(3);
            if (dice == 0)
            {
              while (!queue.offer(item)) {}
            }
            else if (dice == 1)
            {
              queue.put(item);
            }
            else
            {
              while (!queue.put(item, 100, TimeUnit.MILLISECONDS)) {}
            }

            Thread.sleep(r.nextInt(10));
            //System.out.println("[producer] queue size = " + queue.size());
          }
        }
        catch (InterruptedException ex)
        {
          throw new RuntimeException(ex);
        }
      }
    });

    Future<?> f2 = executor.submit(new Runnable()
    {
      public void run()
      {
        try
        {
          BlockingQueue queue = withLocking ? queueMgr.getBlockingQueueWithLocking("myTestQueue") :
            queueMgr.getBlockingQueue("myTestQueue");
          for (int i=0; i<items.size(); i++)
          {
            int dice = r.nextInt(3);
            if (dice == 0)
            {
              while (queue.poll() == null) {}
            }
            else if (dice == 1)
            {
              queue.take();
            }
            else
            {
              while (queue.take(100, TimeUnit.MILLISECONDS) == null) {}
            }

            Thread.sleep(r.nextInt(10));
            //System.out.println("[consumer] queue size = " + queue.size());
          }
        }
        catch (InterruptedException ex)
        {
          throw new RuntimeException(ex);
        }
      }
    });

    Future<?> f3 = executor.submit(new Runnable()
    {
      public void run()
      {
        try
        {
          BlockingQueue queue = withLocking ? queueMgr.getBlockingQueueWithLocking("myTestQueue") :
            queueMgr.getBlockingQueue("myTestQueue");
          for (byte[] item : items)
          {
            int dice = r.nextInt(3);
            if (dice == 0)
            {
              while (!queue.offer(item)) {}
            }
            else if (dice == 1)
            {
              queue.put(item);
            }
            else
            {
              while (!queue.put(item, 100, TimeUnit.MILLISECONDS)) {}
            }

            Thread.sleep(r.nextInt(10));
            //System.out.println("[producer] queue size = " + queue.size());
          }
        }
        catch (InterruptedException ex)
        {
          throw new RuntimeException(ex);
        }
      }
    });

    Future<?> f4 = executor.submit(new Runnable()
    {
      public void run()
      {
        try
        {
          BlockingQueue queue = withLocking ? queueMgr.getBlockingQueueWithLocking("myTestQueue") :
            queueMgr.getBlockingQueue("myTestQueue");
          for (int i=0; i<items.size(); i++)
          {
            int dice = r.nextInt(3);
            if (dice == 0)
            {
              while (queue.poll() == null) {}
            }
            else if (dice == 1)
            {
              queue.take();
            }
            else
            {
              while (queue.take(100, TimeUnit.MILLISECONDS) == null) {}
            }

            Thread.sleep(r.nextInt(10));
            //System.out.println("[consumer] queue size = " + queue.size());
          }
        }
        catch (InterruptedException ex)
        {
          throw new RuntimeException(ex);
        }
      }
    });

    f1.get();
    f2.get();
    f3.get();
    f4.get();

    assertQueueEmpty(queue);
  }
}
