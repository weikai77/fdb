package com.weikai77.fdb.util.concurrent;

import java.util.List;

/**
 * 
 * @author kwei
 *
 */
public interface DistributedQueueFactory
{
  List<Queue> listQueues();
  Queue createQueue(String id);
  Queue createQueue(String id, long capacity);
  Queue getQueue(String id);
  BlockingQueue createBlockingQueue(String id);
  BlockingQueue createBlockingQueue(String id, long capacity);
  BlockingQueue getBlockingQueue(String id);
  void deleteQueue(String id);
}
