package com.weikai77.fdb.util.concurrent;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;
import com.weikai77.fdb.util.FdbUtils;
import com.weikai77.fdb.util.Watch;

/**
 * A distributed condition variable implementation on FDB.
 * <p>
 * The semantics of this implementation is modeled after 
 * {@link java.util.concurrent.locks.Condition}.
 * <p>
 * Layout on FDB:
 * <pre>
 *    /data  -> {@link ConditionData}
 *    /watch -> [RANDOM BYTES]
 * </pre>
 * 
 * @author kwei
 *
 */
public class DistributedCondition
{
  private static final String KEY_WATCH = "watch";
  private static final String KEY_DATA = "data";

  private final Database _fdb;
  private final Subspace _space;
  private final ScheduledExecutorService _timer;
  private final String _id;
  
  // optional
  private final DistributedSimpleLock _lock;
  
  // derived and cached
  private final byte[] _watchKey;
  private final byte[] _dataKey;

  protected DistributedCondition(Database fdb, Subspace space,
      ScheduledExecutorService timer, String id)
  {
    this(fdb, space, timer, id, null);
  }
  
  protected DistributedCondition(Database fdb, Subspace space,
      ScheduledExecutorService timer, String id, DistributedSimpleLock lock)
  {
    this._fdb = fdb;
    this._space = space;
    this._timer = timer;
    this._id = id;
    this._lock = lock;
    this._watchKey = _space.pack(KEY_WATCH);
    this._dataKey = _space.pack(KEY_DATA);
  }
  
  public String getId()
  {
    return _id;
  }
  
  public DistributedSimpleLock getLock()
  {
    return _lock;
  }
  
  /**
   * Waits until this condition gets signaled.
   * 
   * If this condition variable is associated with a lock, the lock must be 
   * acquired prior to calling this method. Upon entry to this method, the
   * lock will be released before this thread goes to sleep as a result of
   * {@code watch.await()}. Before returning from this method, the lock must
   * be re-acquired.
   * 
   * The lock state must not change before and after this method is called.
   * 
   * @throws InterruptedException 
   */
  public void await() throws InterruptedException
  {
    if (_lock != null)
    {
      _lock.release();
    }

    try
    {
      Watch watch = acquireWatch();
      
      try
      {
        watch.await();
      }
      finally
      {
        releaseWatch(watch);
      }
    }
    finally
    {
      if (_lock != null)
      {
        _lock.acquire();
      }
    }
  }
  
  /**
   * Waits until this condition gets signaled or times out.
   * 
   * If this condition variable is associated with a lock, the lock must be 
   * acquired prior to calling this method. Upon entry to this method, the
   * lock will be released before this thread goes to sleep as a result of
   * {@code watch.await()}. Before returning from this method, the lock must
   * be re-acquired.
   * 
   * The lock state must not change before and after this method is called.
   * 
   * @return whether the wait was successful or not
   * @throws InterruptedException 
   */
  public boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException
  {
    if (_lock != null)
    {
      _lock.release();
    }

    try
    {
      Watch watch = acquireWatch();
  
      try
      {
        return watch.await(timeout, timeUnit);
      }
      finally
      {
        releaseWatch(watch);
      }
    }
    finally
    {
      if (_lock != null)
      {
        // this may block forever (not honoring the timeout)
        _lock.acquire();
      }
    }
  }

  private Watch acquireWatch()
  {
    return _fdb.run(new Function<Transaction, Watch>()
    {
      public Watch apply(Transaction tr)
      {
        // increment waiter count
        byte[] dataBytes = tr.get(_dataKey).get();
        ConditionData data = ConditionData.fromBytes(dataBytes);
        if (data == null)
        {
          data = new ConditionData();
        }
        data.incrWaiterCount();
        tr.set(_dataKey, data.toBytes());

        // register the watch
        Future<Void> watch = tr.watch(_watchKey);
        Watch w = new Watch(_watchKey, null, watch, _timer);
        return w;
      }
    });
  }
  
  private void releaseWatch(Watch watch)
  {
    watch.cancel();
    
    // decrement waiter count
    _fdb.run(new Function<Transaction, Void>()
    {
      public Void apply(Transaction tr)
      {
        byte[] dataBytes = tr.get(_dataKey).get();
        ConditionData data = ConditionData.fromBytes(dataBytes);
        if (data != null && data.getWaiterCount() > 0)
        {
          data.decrWaiterCount();
          if (data.getWaiterCount() <= 0)
          {
            // no more waiters, good opportunity to clean up
            tr.clear(_watchKey);
            tr.clear(_dataKey);
          }
          else
          {
            tr.set(_dataKey, data.toBytes());
          }
        }
        return null;
      }
    });
  }
  
  public void signalAll()
  {
    if (_lock != null && !_lock.hasLock())
    {
      throw new IllegalMonitorStateException("You do not have the lock associated "
          + "with this condition variable. Did you acquire the lock before calling me?");
    }

    FdbUtils.signalWatch(_fdb, _watchKey);
  }
  
  private static class ConditionData
  {
    private static final int VERSION = 1;
    private static final int IDX_VERSION = 0;
    private static final int IDX_WAITER_COUNT = 1;

    private final int _version;
    private int _waiterCount;

    public ConditionData()
    {
      this(0);
    }

    public ConditionData(int waiterCount)
    {
      this._version = VERSION;
      this._waiterCount = waiterCount;
    }
    
    public int getWaiterCount() { return _waiterCount; }
    public void incrWaiterCount() { ++_waiterCount; }
    public void decrWaiterCount() { --_waiterCount; }
    
    public byte[] toBytes()
    {
      return Tuple.from(_version, _waiterCount).pack();
    }
    
    public static ConditionData fromBytes(byte[] bytes)
    {
      if (bytes == null)
      {
        return null;
      }
      
      Tuple tuple = Tuple.fromBytes(bytes);
      int version = (int) tuple.getLong(IDX_VERSION);
      if (version == VERSION)
      {
        int waiterCount = ((Number) tuple.get(IDX_WAITER_COUNT)).intValue();
        return new ConditionData(waiterCount);
      }
      else
      {
        throw new IllegalArgumentException("Unsupported version: " + version);
      }
    }
  }
}
