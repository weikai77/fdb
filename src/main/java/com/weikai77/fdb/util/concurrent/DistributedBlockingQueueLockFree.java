package com.weikai77.fdb.util.concurrent;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;
import com.weikai77.fdb.util.FdbUtils;
import com.weikai77.fdb.util.Watch;
import com.weikai77.fdb.util.concurrent.DistributedBoundedQueue.QueueMeta;
import com.weikai77.fdb.util.concurrent.DistributedQueue.QueueItem;

/**
 * A lock-free implementation of distributed blocking queue on FDB.
 * <p>
 * We can go lock-free because of FDB's optimistic concurrency control 
 * and its client's "retry-until-success" model.
 * <p>
 * Based on simple testing, this implementation provides 3x-4x performance
 * improvement over its lock-based variant (see {@link 
 * DistributedBlockingQueue}).
 * <p>
 * Layout on FDB:
 * <pre>
 *    /meta             -> {@link QueueMeta}
 *    /items/0          -> {@link QueueItem}
 *          /1          -> {@link QueueItem} 
 *          /...        -> {@link QueueItem}
 *    /watches/notEmpty -> [RANDOM BYTES]
 *            /notFull  -> [RANDOM BYTES]
 * </pre>
 * 
 * @author kwei
 *
 */
public class DistributedBlockingQueueLockFree implements BlockingQueue
{
  private static final String KEY_WATCHES = "watches";
  private static final String KEY_NOT_EMPTY = "notEmpty";
  private static final String KEY_NOT_FULL = "notFull";

  private final Database _fdb;
  private final DistributedQueue _fifo;
  private final ScheduledExecutorService _timer;
  private final byte[] _notEmpty;
  private final byte[] _notFull;
  
  /**
   * This constructor creates a lock-free instance.
   */
  protected DistributedBlockingQueueLockFree(Database fdb, DistributedQueue fifo,
      ScheduledExecutorService timer)
  {
    _fdb = fdb;
    _fifo = fifo;
    _timer = timer;
    _notEmpty = _fifo.getSpace().subspace(Tuple.from(KEY_WATCHES, KEY_NOT_EMPTY)).pack();
    _notFull = _fifo.getSpace().subspace(Tuple.from(KEY_WATCHES, KEY_NOT_FULL)).pack();
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
    return _fdb.run(new Function<Transaction,byte[]>()
    {
      @Override
      public byte[] apply(Transaction tr)
      {
        byte[] res = _fifo.poll(tr);
        if (res != null)
        {
          FdbUtils.signalWatch(tr, _notFull);
        }
        return res;
      }
    });
  }

  @Override
  public boolean offer(byte[] itemValue)
  {
    return _fdb.run(new Function<Transaction,Boolean>()
    {
      @Override
      public Boolean apply(Transaction tr)
      {
        if (_fifo.offer(tr, itemValue))
        {
          FdbUtils.signalWatch(tr, _notEmpty);
          return true;
        }
        else
        {
          return false;
        }
      }
    });
  }

  @Override
  public byte[] take() throws InterruptedException
  {
    while (true)
    {
      Object res = _fdb.run(new Function<Transaction,Object>()
      {
        @Override
        public Object apply(Transaction tr)
        {
          byte[] item = null;
          if (_fifo.size(tr) <= 0 || (item = _fifo.poll(tr)) == null)
          {
            return FdbUtils.getAndWatch(tr, _timer, _notEmpty);
          }
          else
          {
            // wake up the blocked producers
            FdbUtils.signalWatch(tr, _notFull);
            return item;
          }
        }
      });
      
      if (res instanceof byte[])
      {
        // got an item from the queue
        return (byte[]) res;
      }
      else
      {
        // queue was empty
        Watch watch = (Watch) res;
        try
        {
          watch.await();
        }
        finally
        {
          watch.cancel();
        }
      }
    }
  }

  @Override
  public byte[] take(long timeout, TimeUnit unit) throws InterruptedException
  {
    long startTime = System.currentTimeMillis();
    long timeoutMillis = unit.toMillis(timeout);
    while (true)
    {
      Object res = _fdb.run(new Function<Transaction,Object>()
      {
        @Override
        public Object apply(Transaction tr)
        {
          byte[] item = null;
          if (_fifo.size(tr) <= 0 || (item = _fifo.poll(tr)) == null)
          {
            return FdbUtils.getAndWatch(tr, _timer, _notEmpty);
          }
          else
          {
            // wake up the blocked producers
            FdbUtils.signalWatch(tr, _notFull);
            return item;
          }
        }
      });
      
      if (res instanceof byte[])
      {
        // got an item from the queue
        return (byte[]) res;
      }
      else
      {
        // queue was empty
        Watch watch = (Watch) res;
        try
        {
          long elapsedTime = System.currentTimeMillis() - startTime;
          long remainingTime = timeoutMillis - elapsedTime;
          if (remainingTime <= 0)
          {
            // timed out
            return null;
          }

          boolean timedOut = !watch.await(remainingTime, TimeUnit.MILLISECONDS);
          if (timedOut)
          {
            // timed out
            return null;
          }
        }
        finally
        {
          watch.cancel();
        }
      }
    }
  }

  @Override
  public void put(byte[] itemValue) throws InterruptedException
  {
    while (true)
    {
      Watch watch = _fdb.run(new Function<Transaction,Watch>()
      {
        @Override
        public Watch apply(Transaction tr)
        {
          if (_fifo.isFull(tr) || !_fifo.offer(tr, itemValue))
          {
            return FdbUtils.getAndWatch(tr, _timer, _notFull);
          }
          else
          {
            // wake up the blocked consumers
            FdbUtils.signalWatch(tr, _notEmpty);
            return null;
          }
        }
      });
      
      if (watch == null)
      {
        // the item has been put into the queue
        return;
      }
      else
      {
        // queue was full
        try
        {
          watch.await();
        }
        finally
        {
          watch.cancel();
        }
      }
    }
  }
  
  @Override
  public boolean put(byte[] itemValue, long timeout, TimeUnit unit) throws InterruptedException
  {
    long startTime = System.currentTimeMillis();
    long timeoutMillis = unit.toMillis(timeout);
    while (true)
    {
      Watch watch = _fdb.run(new Function<Transaction,Watch>()
      {
        @Override
        public Watch apply(Transaction tr)
        {
          if (_fifo.isFull(tr) || !_fifo.offer(tr, itemValue))
          {
            return FdbUtils.getAndWatch(tr, _timer, _notFull);
          }
          else
          {
            // wake up the blocked consumers
            FdbUtils.signalWatch(tr, _notEmpty);
            return null;
          }
        }
      });
      
      if (watch == null)
      {
        // the item has been put into the queue
        return true;
      }
      else
      {
        // queue was full
        try
        {
          long elapsedTime = System.currentTimeMillis() - startTime;
          long remainingTime = timeoutMillis - elapsedTime;
          if (remainingTime <= 0)
          {
            // timed out
            return false;
          }

          boolean timedOut = !watch.await(remainingTime, TimeUnit.MILLISECONDS);
          if (timedOut)
          {
            // timed out
            return false;
          }
        }
        finally
        {
          watch.cancel();
        }
      }
    }
  }
  
}
