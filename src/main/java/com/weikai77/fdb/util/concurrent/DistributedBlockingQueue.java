package com.weikai77.fdb.util.concurrent;

import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.foundationdb.Database;
import com.foundationdb.tuple.Tuple;
import com.weikai77.fdb.util.concurrent.DistributedBoundedQueue.QueueMeta;
import com.weikai77.fdb.util.concurrent.DistributedQueue.QueueItem;

/**
 * A distributed blocking queue implementation on FDB.
 * <p>
 * Layout on FDB:
 * <pre>
 *    /meta             -> {@link QueueMeta}
 *    /items/0          -> {@link QueueItem} 
 *          /1          -> {@link QueueItem} 
 *          /...        -> {@link QueueItem} 
 *    /lock             -> {@link DistributedSimpleLock}
 * </pre>
 * 
 * @author kwei
 *
 */
public class DistributedBlockingQueue implements BlockingQueue
{
  private static final String KEY_LOCK = "lock";
  private static final String CONDITION_NOT_EMPTY = "notEmpty";
  private static final String CONDITION_NOT_FULL = "notFull";

  private final Database _fdb;
  private final DistributedQueue _fifo;
  private final DistributedSimpleLock _lock;
  private final DistributedCondition _notEmpty;
  private final DistributedCondition _notFull;
  
  protected DistributedBlockingQueue(Database fdb, ScheduledExecutorService timer, 
      DistributedQueue fifo)
  {
    _fdb = fdb;
    _fifo = fifo;
    _lock = new DistributedSimpleLock(_fdb, _fifo.getSpace().subspace(Tuple.from(KEY_LOCK)), 
        timer, KEY_LOCK, UUID.randomUUID().toString());
    _notEmpty = _lock.newCondition(CONDITION_NOT_EMPTY);
    _notFull = _lock.newCondition(CONDITION_NOT_FULL);
  }

  @Override
  public String getId()
  {
    return _fifo.getId();
  }

  @Override
  public long size()
  {
    return _fifo.size();
  }
  
  @Override
  public long capacity()
  {
    return _fifo.capacity();
  }
  
  @Override
  public boolean isEmpty()
  {
    return _fifo.isEmpty();
  }
  
  @Override
  public boolean isFull()
  {
    return _fifo.isFull();
  }
  
  @Override
  public byte[] poll()
  {
    _lock.acquire();
    try
    {
      byte[] res = _fifo.poll();
      if (res != null)
      {
        _notFull.signalAll();
      }
  
      return res;
    }
    finally
    {
      _lock.release();
    }
  }

  @Override
  public boolean offer(byte[] itemValue)
  {
    _lock.acquire();
    try
    {
      if (_fifo.offer(itemValue))
      {
        _notEmpty.signalAll();
        return true;
      }
      else
      {
        return false;
      }
    }
    finally
    {
      _lock.release();
    }
  }

  @Override
  public byte[] take() throws InterruptedException
  {
    _lock.acquire();
    try
    {
      byte[] item = null;
      while (_fifo.size() <= 0 || (item = _fifo.poll()) == null)
      {
        _notEmpty.await();
      }
      
      if (!_fifo.isFull())
      {
        _notFull.signalAll();
      }
      
      return item;
    }
    finally
    {
      _lock.release();
    }
  }

  @Override
  public byte[] take(long timeout, TimeUnit unit) throws InterruptedException
  {
    long startTime = System.currentTimeMillis();
    long timeoutMillis = unit.toMillis(timeout);

    if (!_lock.tryAcquire(timeoutMillis))
    {
      // timed out acquiring the lock
      return null;
    }

    try
    {
      byte[] item = null;
      while (_fifo.size() <= 0 || (item = _fifo.poll()) == null)
      {
        long elapsedTime = System.currentTimeMillis() - startTime;
        long remainingTime = timeoutMillis - elapsedTime;
        if (remainingTime <= 0)
        {
          // timed out
          return null;
        }

        boolean timedOut = !_notEmpty.await(remainingTime, TimeUnit.MILLISECONDS);
        if (timedOut)
        {
          // timed out
          return null;
        }
      }
      
      if (!_fifo.isFull())
      {
        _notFull.signalAll();
      }
      
      return item;
    }
    finally
    {
      _lock.release();
    }
  }

  @Override
  public void put(byte[] itemValue) throws InterruptedException
  {
    _lock.acquire();
    try
    {
      while (_fifo.isFull() || !_fifo.offer(itemValue))
      {
        _notFull.await();
      }
  
      _notEmpty.signalAll();
    }
    finally
    {
      _lock.release();
    }
  }
  
  @Override
  public boolean put(byte[] itemValue, long timeout, TimeUnit unit) throws InterruptedException
  {
    long startTime = System.currentTimeMillis();
    long timeoutMillis = unit.toMillis(timeout);

    if (!_lock.tryAcquire(timeoutMillis))
    {
      // timed out acquiring the lock
      return false;
    }

    try
    {
      while (_fifo.isFull() || !_fifo.offer(itemValue))
      {
        long elapsedTime = System.currentTimeMillis() - startTime;
        long remainingTime = timeoutMillis - elapsedTime;
        if (remainingTime <= 0)
        {
          // timed out
          return false;
        }

        boolean timedOut = !_notFull.await(remainingTime, TimeUnit.MILLISECONDS);
        if (timedOut)
        {
          // timed out
          return false;
        }
      }
  
      _notEmpty.signalAll();
      return true;
    }
    finally
    {
      _lock.release();
    }
  }
  
  /**
   * Just for fun, and reference implementation for distributed version
   * (See {@link DistributedBlockingQueue}).
   */
  public static class BlockingQueue<T>
  {
    private final Lock _lock;
    private final Condition _notEmpty;
    private final Condition _notFull;
    private final int _capacity;
    private final LinkedList<T> _fifo;
    
    public BlockingQueue(int capacity)
    {
      _lock = new ReentrantLock();
      _notEmpty = _lock.newCondition();
      _notFull = _lock.newCondition();
      _capacity = capacity;
      _fifo = new LinkedList<T>();
    }
    
    public T take() throws InterruptedException
    {
      _lock.lock();
      try
      {
        while (_fifo.isEmpty())
        {
          _notEmpty.await();
        }
        
        T item = _fifo.removeFirst();
        
        if (_fifo.size() < _capacity)
        {
          _notFull.signal();
        }

        return item;
      }
      finally
      {
        _lock.unlock();
      }
    }
    
    public void put(T element) throws InterruptedException
    {
      _lock.lock();
      try
      {
        while (_fifo.size() >= _capacity)
        {
          _notFull.await();
        }
        
        _fifo.addLast(element);
        _notEmpty.signal();
      }
      finally
      {
        _lock.unlock();
      }
    }
  }
}
