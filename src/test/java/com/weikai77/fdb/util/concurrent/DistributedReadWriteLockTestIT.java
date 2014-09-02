package com.weikai77.fdb.util.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

import com.weikai77.fdb.util.concurrent.DistributedReadWriteLock.ReadLock;
import com.weikai77.fdb.util.concurrent.DistributedReadWriteLock.WriteLock;

/**
 * 
 * @author kwei
 *
 */
public class DistributedReadWriteLockTestIT
{
  @Test
  public void testSingleThreaded() throws Exception
  {
    DistributedLockMgr lockMgr = TestUtils.getLockMgr();
    DistributedReadWriteLock rwl = lockMgr.newReadWriteLock("myTestReadWriteLock");
    
    try
    {
      ReadLock readLock = rwl.readLock();
      WriteLock writeLock = rwl.writeLock();
  
      Assert.assertEquals(0, rwl.getReaderCount());
      Assert.assertEquals(0, rwl.getWriterCount());
      readLock.lock();
      Assert.assertEquals(1, rwl.getReaderCount());
      Assert.assertEquals(0, rwl.getWriterCount());
      readLock.lock();
      Assert.assertEquals(2, rwl.getReaderCount());
      Assert.assertEquals(0, rwl.getWriterCount());
      readLock.unlock();
      Assert.assertEquals(1, rwl.getReaderCount());
      Assert.assertEquals(0, rwl.getWriterCount());
      readLock.lock();
      Assert.assertEquals(2, rwl.getReaderCount());
      Assert.assertEquals(0, rwl.getWriterCount());
      readLock.unlock();
      Assert.assertEquals(1, rwl.getReaderCount());
      Assert.assertEquals(0, rwl.getWriterCount());
      readLock.unlock();
      Assert.assertEquals(0, rwl.getReaderCount());
      Assert.assertEquals(0, rwl.getWriterCount());
      writeLock.lock();
      Assert.assertEquals(0, rwl.getReaderCount());
      Assert.assertEquals(1, rwl.getWriterCount());
      writeLock.unlock();
      Assert.assertEquals(0, rwl.getReaderCount());
      Assert.assertEquals(0, rwl.getWriterCount());
    }
    finally
    {
      lockMgr.deleteReadWriteLock("myTestReadWriteLock");
    }
  }

  @Test
  public void testMultiThreaded() throws Exception
  {
    DistributedLockMgr lockMgr = TestUtils.getLockMgr();
    DistributedReadWriteLock rwl = lockMgr.newReadWriteLock("myTestReadWriteLock");
    
    try
    {
      Assert.assertEquals(0, rwl.getReaderCount());
      Assert.assertEquals(0, rwl.getWriterCount());
  
      ExecutorService executor = Executors.newFixedThreadPool(2);
      Future<?> f1 = executor.submit(new Runnable()
      {
        public void run()
        {
          try
          {
            DistributedReadWriteLock rwl = lockMgr.newReadWriteLock("myTestReadWriteLock");
            ReadLock readLock = rwl.readLock();
            WriteLock writeLock = rwl.writeLock();
  
            writeLock.lock();
            writeLock.unlock();
            readLock.lock();
            readLock.lock();
            readLock.unlock();
            readLock.lock();
            readLock.unlock();
            readLock.unlock();
            writeLock.lock();
            writeLock.unlock();
          }
          catch (Exception ex)
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
            DistributedReadWriteLock rwl = lockMgr.newReadWriteLock("myTestReadWriteLock");
            ReadLock readLock = rwl.readLock();
            WriteLock writeLock = rwl.writeLock();
  
            readLock.lock();
            readLock.unlock();
            writeLock.lock();
            writeLock.unlock();
            writeLock.lock();
            writeLock.unlock();
            writeLock.lock();
            writeLock.unlock();
            readLock.lock();
            readLock.unlock();
          }
          catch (Exception ex)
          {
            throw new RuntimeException(ex);
          }
        }
      });
  
      f1.get();
      f2.get();
  
      Assert.assertEquals(0, rwl.getReaderCount());
      Assert.assertEquals(0, rwl.getWriterCount());
    }
    finally
    {
      lockMgr.deleteReadWriteLock("myTestReadWriteLock");
    }
  }
}
