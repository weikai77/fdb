package com.weikai77.fdb.util.concurrent;

/**
 * 
 * @author kwei
 *
 */
public interface DistributedLockFactory
{
  DistributedSimpleLock newSimpleLock(String id);
  DistributedReentrantLock newReentrantLock(String id);
  DistributedReadWriteLock newReadWriteLock(String id);
  DistributedCountDownLatch newCountDownLatch(String id);
}
