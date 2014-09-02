package com.weikai77.fdb.util.concurrent;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author kwei
 *
 */
public class DistributedQueueMgrTestIT 
{
  @Test
  public void testCRUD() throws Exception
  {
    testCRUD(TestUtils.getTupleBasedQueueMgr());
    testCRUD(TestUtils.getDirectoryBasedQueueMgr());
  }
  
  private void testCRUD(DistributedQueueMgr mgr) throws Exception
  {
    Assert.assertTrue(mgr.listQueues().isEmpty());
    try
    {
      List<Queue> queues = null;

      // Queue 1
      Queue queue1 = mgr.createQueue("queue1");
      Assert.assertEquals("queue1", queue1.getId());
      Assert.assertTrue(queue1.capacity() <= 0);
      Assert.assertTrue(queue1.size() == 0);
      Assert.assertTrue(queue1.isEmpty());
      Assert.assertFalse(queue1.isFull());

      queue1 = mgr.getQueue("queue1");
      Assert.assertNotNull(queue1);
      Assert.assertEquals("queue1", queue1.getId());
      Assert.assertTrue(queue1.capacity() <= 0);
      Assert.assertTrue(queue1.size() == 0);
      Assert.assertTrue(queue1.isEmpty());
      Assert.assertFalse(queue1.isFull());
      queue1 = mgr.getBlockingQueue("queue1");
      Assert.assertNotNull(queue1);
      Assert.assertEquals("queue1", queue1.getId());
      Assert.assertTrue(queue1.capacity() <= 0);
      Assert.assertTrue(queue1.size() == 0);
      Assert.assertTrue(queue1.isEmpty());
      Assert.assertFalse(queue1.isFull());

      queues = mgr.listQueues();
      Assert.assertEquals(1, queues.size());
      queue1 = queues.get(0);
      Assert.assertNotNull(queue1);
      Assert.assertEquals("queue1", queue1.getId());
      Assert.assertTrue(queue1.capacity() <= 0);
      Assert.assertTrue(queue1.size() == 0);
      Assert.assertTrue(queue1.isEmpty());
      Assert.assertFalse(queue1.isFull());

      try
      {
        mgr.createQueue("queue1");
        Assert.fail("Should have failed but did not");
      }
      catch (IllegalArgumentException ex) {}
      
      mgr.deleteQueue("queue1");
      Assert.assertEquals(0, mgr.listQueues().size());
      Assert.assertNull(mgr.getQueue("queue1"));
      Assert.assertNull(mgr.getBlockingQueue("queue1"));

      // Queue 2
      Queue queue2 = mgr.createQueue("queue2", 100);
      Assert.assertEquals("queue2", queue2.getId());
      Assert.assertTrue(queue2.capacity() == 100);
      Assert.assertTrue(queue2.size() == 0);
      Assert.assertTrue(queue2.isEmpty());
      Assert.assertFalse(queue2.isFull());

      queue2 = mgr.getQueue("queue2");
      Assert.assertNotNull(queue2);
      Assert.assertEquals("queue2", queue2.getId());
      Assert.assertTrue(queue2.capacity() == 100);
      Assert.assertTrue(queue2.size() == 0);
      Assert.assertTrue(queue2.isEmpty());
      Assert.assertFalse(queue2.isFull());
      queue2 = mgr.getBlockingQueue("queue2");
      Assert.assertNotNull(queue2);
      Assert.assertEquals("queue2", queue2.getId());
      Assert.assertTrue(queue2.capacity() == 100);
      Assert.assertTrue(queue2.size() == 0);
      Assert.assertTrue(queue2.isEmpty());
      Assert.assertFalse(queue2.isFull());

      queues = mgr.listQueues();
      Assert.assertEquals(1, queues.size());

      // Queue 3
      Queue queue3 = mgr.createBlockingQueue("queue3");
      Assert.assertEquals("queue3", queue3.getId());
      Assert.assertTrue(queue3.capacity() <= 0);
      Assert.assertTrue(queue3.size() == 0);
      Assert.assertTrue(queue3.isEmpty());
      Assert.assertFalse(queue3.isFull());

      queue3 = mgr.getQueue("queue3");
      Assert.assertNotNull(queue3);
      Assert.assertEquals("queue3", queue3.getId());
      Assert.assertTrue(queue3.capacity() <= 0);
      Assert.assertTrue(queue3.size() == 0);
      Assert.assertTrue(queue3.isEmpty());
      Assert.assertFalse(queue3.isFull());
      queue3 = mgr.getBlockingQueue("queue3");
      Assert.assertNotNull(queue3);
      Assert.assertEquals("queue3", queue3.getId());
      Assert.assertTrue(queue3.capacity() <= 0);
      Assert.assertTrue(queue3.size() == 0);
      Assert.assertTrue(queue3.isEmpty());
      Assert.assertFalse(queue3.isFull());

      queues = mgr.listQueues();
      Assert.assertEquals(2, queues.size());

      // Queue 4
      Queue queue4 = mgr.createBlockingQueue("queue4", 100);
      Assert.assertEquals("queue4", queue4.getId());
      Assert.assertTrue(queue4.capacity() == 100);
      Assert.assertTrue(queue4.size() == 0);
      Assert.assertTrue(queue4.isEmpty());
      Assert.assertFalse(queue4.isFull());

      queue4 = mgr.getQueue("queue4");
      Assert.assertNotNull(queue4);
      Assert.assertEquals("queue4", queue4.getId());
      Assert.assertTrue(queue4.capacity() == 100);
      Assert.assertTrue(queue4.size() == 0);
      Assert.assertTrue(queue4.isEmpty());
      Assert.assertFalse(queue4.isFull());
      queue4 = mgr.getBlockingQueue("queue4");
      Assert.assertNotNull(queue4);
      Assert.assertEquals("queue4", queue4.getId());
      Assert.assertTrue(queue4.capacity() == 100);
      Assert.assertTrue(queue4.size() == 0);
      Assert.assertTrue(queue4.isEmpty());
      Assert.assertFalse(queue4.isFull());

      queues = mgr.listQueues();
      Assert.assertEquals(3, queues.size());

      // Queue 5
      Queue queue5 = mgr.createBlockingQueueWithLocking("queue5", 200);
      Assert.assertEquals("queue5", queue5.getId());
      Assert.assertTrue(queue5.capacity() == 200);
      Assert.assertTrue(queue5.size() == 0);
      Assert.assertTrue(queue5.isEmpty());
      Assert.assertFalse(queue5.isFull());

      queue5 = mgr.getQueue("queue5");
      Assert.assertNotNull(queue5);
      Assert.assertEquals("queue5", queue5.getId());
      Assert.assertTrue(queue5.capacity() == 200);
      Assert.assertTrue(queue5.size() == 0);
      Assert.assertTrue(queue5.isEmpty());
      Assert.assertFalse(queue5.isFull());
      queue5 = mgr.getBlockingQueue("queue5");
      Assert.assertNotNull(queue5);
      Assert.assertEquals("queue5", queue5.getId());
      Assert.assertTrue(queue5.capacity() == 200);
      Assert.assertTrue(queue5.size() == 0);
      Assert.assertTrue(queue5.isEmpty());
      Assert.assertFalse(queue5.isFull());

      queues = mgr.listQueues();
      Assert.assertEquals(4, queues.size());

      queue2 = queues.get(0);
      queue3 = queues.get(1);
      queue4 = queues.get(2);
      queue5 = queues.get(3);
      Assert.assertEquals("queue2", queue2.getId());
      Assert.assertTrue(queue2.capacity() == 100);
      Assert.assertTrue(queue2.size() == 0);
      Assert.assertTrue(queue2.isEmpty());
      Assert.assertFalse(queue2.isFull());
      Assert.assertEquals("queue3", queue3.getId());
      Assert.assertTrue(queue3.capacity() <= 0);
      Assert.assertTrue(queue3.size() == 0);
      Assert.assertTrue(queue3.isEmpty());
      Assert.assertFalse(queue3.isFull());
      Assert.assertEquals("queue4", queue4.getId());
      Assert.assertTrue(queue4.capacity() == 100);
      Assert.assertTrue(queue4.size() == 0);
      Assert.assertTrue(queue4.isEmpty());
      Assert.assertFalse(queue4.isFull());
      Assert.assertEquals("queue5", queue5.getId());
      Assert.assertTrue(queue5.capacity() == 200);
      Assert.assertTrue(queue5.size() == 0);
      Assert.assertTrue(queue5.isEmpty());
      Assert.assertFalse(queue5.isFull());
    }
    finally
    {
      mgr.deleteQueue("queue1");
      mgr.deleteQueue("queue2");
      mgr.deleteQueue("queue3");
      mgr.deleteQueue("queue4");
      mgr.deleteQueue("queue5");
      Assert.assertTrue(mgr.listQueues().isEmpty());
    }
  }
  
}
