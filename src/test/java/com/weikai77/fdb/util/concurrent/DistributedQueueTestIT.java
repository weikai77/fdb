package com.weikai77.fdb.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author kwei
 *
 */
public class DistributedQueueTestIT
{

  @Before
  public void setUp() throws Exception
  {
    TestUtils.getTupleBasedQueueMgr().createQueue("myTestQueue");
    TestUtils.getDirectoryBasedQueueMgr().createQueue("myTestQueue");
  }
  
  @After
  public void tearDown() throws Exception
  {
    TestUtils.getTupleBasedQueueMgr().deleteQueue("myTestQueue");
    TestUtils.getDirectoryBasedQueueMgr().deleteQueue("myTestQueue");
  }
  
  @Test
  public void testSingleThreadedTupleBased() throws Exception
  {
    testSingleThreaded(TestUtils.getTupleBasedQueueMgr());
  }
  
  @Test
  public void testSingleThreadedDirectoryBased() throws Exception
  {
    testSingleThreaded(TestUtils.getDirectoryBasedQueueMgr());
  }
  
  private void testSingleThreaded(DistributedQueueMgr queueMgr) throws Exception
  {
    DistributedQueue queue = queueMgr.getQueue("myTestQueue");
    Assert.assertEquals(0l, queue.size());
    Assert.assertNull(queue.poll());

    byte[] foo = "foo".getBytes();
    Assert.assertTrue(queue.offer(foo));
    Assert.assertEquals(1l, queue.size());
    Assert.assertArrayEquals(foo, queue.poll());
    Assert.assertEquals(0l, queue.size());

    Assert.assertNull(queue.poll());

    Random r = new Random();
    List<byte[]> items = new ArrayList<byte[]>();
    for (int i=0; i<100; i++)
    {
      byte[] bytes = new byte[10];
      r.nextBytes(bytes);
      items.add(bytes);
    }

    int size = 0;
    for (byte[] item : items)
    {
      queue.offer(item);
      Assert.assertEquals(++size, queue.size());
    }
    
    for (byte[] item : items)
    {
      Assert.assertEquals(size--, queue.size());
      Assert.assertArrayEquals(item, queue.poll());
    }

    Assert.assertEquals(0l, queue.size());
    Assert.assertNull(queue.poll());
  }
  
  @Test
  public void testMultiThreadedTupleBased() throws Exception
  {
    testMultiThreaded(TestUtils.getTupleBasedQueueMgr());
  }

  @Test
  public void testMultiThreadedDirectoryBased() throws Exception
  {
    testMultiThreaded(TestUtils.getDirectoryBasedQueueMgr());
  }
  
  private void testMultiThreaded(DistributedQueueMgr queueMgr) throws Exception
  {
    Random r = new Random();
    List<byte[]> items = new ArrayList<byte[]>();
    for (int i=0; i<100; i++)
    {
      byte[] bytes = new byte[10];
      r.nextBytes(bytes);
      items.add(bytes);
    }

    DistributedQueue queue = queueMgr.getQueue("myTestQueue");
    Assert.assertEquals(0l, queue.size());
    Assert.assertNull(queue.poll());

    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<?> f1 = executor.submit(new Runnable()
    {
      public void run()
      {
        DistributedQueue queue = queueMgr.getQueue("myTestQueue");
        for (byte[] item : items)
        {
          while (!queue.offer(item)) {}
        }
      }
    });

    Future<?> f2 = executor.submit(new Runnable()
    {
      public void run()
      {
        DistributedQueue queue = queueMgr.getQueue("myTestQueue");
        for (byte[] item : items)
        {
          byte[] taken = null;
          while ((taken = queue.poll()) == null) {}

          Assert.assertArrayEquals(item, taken);
        }
      }
    });

    f1.get();
    f2.get();

    Assert.assertEquals(0l, queue.size());
    Assert.assertNull(queue.poll());
  }
}
