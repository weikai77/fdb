package com.weikai77.fdb.util.concurrent;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;
import com.weikai77.fdb.util.FdbUtils;
import com.weikai77.fdb.util.Watch;
import com.weikai77.util.Clock;
import com.weikai77.util.SystemClock;

/**
 * A lock-free, distributed CountDownLatch implementation on FDB.
 * <p>
 * Layout on FDB:
 * <pre>
 *    /data    -> {@link CountDownData}
 *    /watch   -> [RANDOM BYTES]
 * </pre>
 * 
 * @author kwei
 *
 */
public class DistributedCountDownLatch
{
  private static final String KEY_DATA = "data";
  private static final String KEY_WATCH = "watch";

  private final Database _fdb;
  private final Subspace _space;
  private final String _id;
  private final ScheduledExecutorService _timer;

  // derived and cached
  private final byte[] _dataKey;
  private final byte[] _watchKey;

  public DistributedCountDownLatch(Database fdb, Subspace space, 
      ScheduledExecutorService timer, String id)
  {
    this._fdb = fdb;
    this._space = space;
    this._id = id;
    this._timer = timer;
    this._dataKey = _space.pack(KEY_DATA);
    this._watchKey = _space.pack(KEY_WATCH);
  }
  
  public String getId()
  {
    return _id;
  }
  
  public void setCount(int count)
  {
    _fdb.run(new Function<Transaction,Void>()
    {
      @Override
      public Void apply(Transaction tr)
      {
        CountDownData data = getData(tr);
        if (data == null)
        {
          data = new CountDownData(count);
        }
        else
        {
          data.setCount(count);
        }
        
        tr.set(_dataKey, data.toBytes());
        
        if (count == 0)
        {
          FdbUtils.signalWatch(tr, _watchKey);
        }

        return null;
      }
    });
  }
  
  public int getCount()
  {
    return _fdb.run(new Function<Transaction,Integer>()
    {
      @Override
      public Integer apply(Transaction tr)
      {
        CountDownData data = getData(tr);
        return data == null ? 0 : data.getCount();
      }
    });
  }
  
  public void countDown()
  {
    _fdb.run(new Function<Transaction,Void>()
    {
      @Override
      public Void apply(Transaction tr)
      {
        CountDownData data = getData(tr);
        if (data == null)
        {
          return null;
        }

        data.countDown();
        tr.set(_dataKey, data.toBytes());
        
        if (data.getCount() == 0)
        {
          FdbUtils.signalWatch(tr, _watchKey);
        }

        return null;
      }
    });
  }
  
  public void await()
  {
    while (true)
    {
      Watch watch = _fdb.run(new Function<Transaction,Watch>()
      {
        @Override
        public Watch apply(Transaction tr)
        {
          CountDownData data = getData(tr);
          if (data != null && data.getCount() > 0)
          {
            return FdbUtils.getAndWatch(tr, _timer, _watchKey);
          }
          else
          {
            return null;
          }
        }
      });
      
      if (watch == null)
      {
        // count was zero
        return;
      }
      else
      {
        // count was greater than zero
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
  
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException
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
          CountDownData data = getData(tr);
          if (data != null && data.getCount() > 0)
          {
            return FdbUtils.getAndWatch(tr, _timer, _watchKey);
          }
          else
          {
            return null;
          }
        }
      });
      
      if (watch == null)
      {
        // count was zero
        return true;
      }
      else
      {
        try
        {
          // count was greater than zero
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
  
  private CountDownData getData(Transaction tr)
  {
    byte[] bytes = tr.get(_dataKey).get();
    return CountDownData.fromBytes(bytes);
  }
  
  private static class CountDownData
  {
    private static final int VERSION = 1;
    private static final int IDX_VERSION = 0;
    private static final int IDX_COUNT = 1;

    private final int _version;
    private int _count;
    
    public CountDownData(int count)
    {
      this._version = VERSION;
      this._count = count;
    }
    
    public int getCount() { return _count; }
    public void setCount(int count) { _count = count; }
    public void countDown() { if (_count > 0) --_count; }
    
    public byte[] toBytes()
    {
      return Tuple.from(_version, _count).pack();
    }
    
    public static CountDownData fromBytes(byte[] bytes)
    {
      if (bytes == null)
      {
        return null;
      }
      
      Tuple tuple = Tuple.fromBytes(bytes);
      int version = (int) tuple.getLong(IDX_VERSION);
      if (version == VERSION)
      {
        int count = (int) tuple.getLong(IDX_COUNT);
        return new CountDownData(count);
      }
      else
      {
        throw new IllegalArgumentException("Unsupported version: " + version);
      }
    }
  }
  
  /**
   * Just for fun, and a reference implementation for the distributed
   * version (see {@link DistributedCountDownLatch}).
   */
  public static class CountDownLatch
  {
    private final Lock _lock;
    private final Condition _zero;
    private volatile int _count;
    private final Clock _clock;
    
    public CountDownLatch(int count, Clock clock)
    {
      _lock = new ReentrantLock();
      _zero = _lock.newCondition();
      _count = count;
      _clock = clock;
    }
    
    public CountDownLatch(int count)
    {
      this(count, SystemClock.getInstance());
    }
    
    public int getCount()
    {
      return _count;
    }
    
    public void countDown()
    {
      _lock.lock();
      try
      {
        if (_count > 0)
        {
          --_count;
        }

        if (_count <= 0)
        {
          _zero.signalAll();
        }
      }
      finally
      {
        _lock.unlock();
      }
    }
    
    public void await() throws InterruptedException
    {
      _lock.lock();
      try
      {
        while (_count > 0)
        {
          _zero.await();
        }
      }
      finally
      {
        _lock.unlock();
      }
    }

    public boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException
    {
      long startTime = _clock.currentTimeMillis();
      long timeoutMillis = timeUnit.toMillis(timeout);
      if (_lock.tryLock(timeout, timeUnit) == false)
      {
        return false;
      }

      try
      {
        while (_count > 0)
        {
          long elapsedTime = _clock.currentTimeMillis() - startTime;
          long remainingTime = timeoutMillis - elapsedTime;
          if (remainingTime <= 0)
          {
            // timed out
            return false;
          }

          boolean timedOut = !_zero.await(remainingTime, TimeUnit.MILLISECONDS);
          if (timedOut)
          {
            // timed out
            return false;
          }
        }

        // exited while loop without timing out
        return true;
      }
      finally
      {
        _lock.unlock();
      }
    }
  }
}
