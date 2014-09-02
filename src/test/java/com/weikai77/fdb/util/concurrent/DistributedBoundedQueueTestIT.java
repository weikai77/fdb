package com.weikai77.fdb.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author kwei
 *
 */
public class DistributedBoundedQueueTestIT
{
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
    // a queue of capacity 1
    queueMgr.createQueue("myTestQueue", 1);
    try
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
      for (int i=0; i<10; i++)
      {
        byte[] item = new byte[10];
        r.nextBytes(item);
        Assert.assertTrue(queue.offer(item));
        Assert.assertEquals(1l, queue.size());
        Assert.assertArrayEquals(item, queue.poll());
        Assert.assertEquals(0l, queue.size());
      }
  
      for (int i=0; i<10; i++)
      {
        Assert.assertNull(queue.poll());
        Assert.assertEquals(0, queue.size());
      }
  
      Assert.assertTrue(queue.offer(foo));
      for (int i=0; i<10; i++)
      {
        byte[] item = new byte[10];
        r.nextBytes(item);
        //System.out.println("i = " + i);
        Assert.assertFalse(queue.offer(item));
        Assert.assertEquals(1, queue.size());
      }
      
      Assert.assertNotNull(queue.poll());
      Assert.assertNull(queue.poll());
      Assert.assertEquals(0l, queue.size());
    }
    finally
    {
      queueMgr.deleteQueue("myTestQueue");
    }

    // a queue of capacity 2
    queueMgr.createQueue("myTestQueue", 2);
    try
    {
      DistributedQueue queue = queueMgr.getQueue("myTestQueue");
      Assert.assertEquals(0l, queue.size());
      Assert.assertNull(queue.poll());
  
      byte[] one = "one".getBytes();
      Assert.assertTrue(queue.offer(one));
      Assert.assertEquals(1l, queue.size());
      byte[] two = "two".getBytes();
      Assert.assertTrue(queue.offer(two));
      Assert.assertEquals(2l, queue.size());

      Assert.assertArrayEquals(one, queue.poll());
      Assert.assertEquals(1l, queue.size());

      byte[] three = "three".getBytes();
      Assert.assertTrue(queue.offer(three));
      Assert.assertEquals(2l, queue.size());
  
      Assert.assertArrayEquals(two, queue.poll());
      Assert.assertEquals(1l, queue.size());
      Assert.assertArrayEquals(three, queue.poll());
      Assert.assertEquals(0l, queue.size());

      byte[] four = "four".getBytes();
      Assert.assertTrue(queue.offer(four));
      Assert.assertEquals(1l, queue.size());
      Assert.assertArrayEquals(four, queue.poll());
      Assert.assertEquals(0l, queue.size());
    }
    finally
    {
      queueMgr.deleteQueue("myTestQueue");
    }
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

    queueMgr.createQueue("myTestQueue", 10);
    try
    {
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
            try { Thread.sleep(r.nextInt(10)); } catch (InterruptedException ex) {}
            //System.out.println("queue size = " + queue.size());
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
            try { Thread.sleep(r.nextInt(20)); } catch (InterruptedException ex) {}
            //System.out.println("queue size = " + queue.size());
  
            Assert.assertArrayEquals(item, taken);
          }
        }
      });
  
      f1.get();
      f2.get();
  
      Assert.assertEquals(0l, queue.size());
      Assert.assertNull(queue.poll());
    }
    finally
    {
      queueMgr.deleteQueue("myTestQueue");
    }
  }
}
