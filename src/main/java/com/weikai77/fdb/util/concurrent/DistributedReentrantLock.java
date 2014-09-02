package com.weikai77.fdb.util.concurrent;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;
import com.weikai77.fdb.util.FdbUtils;
import com.weikai77.fdb.util.Watch;

/**
 * A distributed, reentrant lock implementation on FDB.
 * <p>
 * The semantics of this implementation is modeled after
 * {@link java.util.concurrent.locks.ReentrantLock}.
 * <p>
 * Layout on FDB:
 * <pre>
 *    /data   -> {@link LockData}
 * </pre>
 * 
 * @author kwei
 *
 */
public class DistributedReentrantLock
{
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedReentrantLock.class);

  private static final String KEY_DATA = "data";

  private final Database _fdb;
  private final Subspace _space;
  private final ScheduledExecutorService _timer;
  private final String _id;
  private final String _owner;

  // derived and cached
  private final byte[] _dataKey;
  
  protected DistributedReentrantLock(Database db, Subspace space, ScheduledExecutorService timer, 
      String id, String owner)
  {
    this._fdb = db;
    this._space = space;
    this._timer = timer;
    this._id = id;
    this._owner = owner;
    this._dataKey = _space.pack(KEY_DATA);
  }
  
  public String getId()
  {
    return _id;
  }
  
  public boolean tryAcquire()
  {
    return acquireIfAvailable();
  }

  public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException
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
          if (acquireIfAvailable(tr))
          {
            return null;
          }
          else
          {
            return FdbUtils.getAndWatch(tr, _timer, _dataKey);
          }
        }
      });

      if (watch == null)
      {
        // acquired the lock
        return true;
      }
      else
      {
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

  public void acquire()
  {
    while (true)
    {
      Watch watch = _fdb.run(new Function<Transaction,Watch>()
      {
        @Override
        public Watch apply(Transaction tr)
        {
          if (acquireIfAvailable(tr))
          {
            return null;
          }
          else
          {
            return FdbUtils.getAndWatch(tr, _timer, _dataKey);
          }
        }
      });

      if (watch == null)
      {
        // acquired the lock
        return;
      }
      else
      {
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
  
  private boolean acquireIfAvailable()
  {
    return _fdb.run(new Function<Transaction, Boolean>()
    {
      public Boolean apply(Transaction tr)
      {
        return acquireIfAvailable(tr);
      }
    });
  }
  
  private boolean acquireIfAvailable(Transaction tr)
  {
    LockData lock = getLock(tr);
    if (lock == null)
    {
      // lock is free
      createLockInFdb(tr);
      return true;
    }
    else if (_owner.equals(lock.getOwner()))
    {
      // already locked by me
      incrementHoldCountInFdb(tr, lock);
      return true;
    }
    else
    {
      return false;
    }
  }
  
  /**
   * Persists this lock to FDB.
   */
  private void createLockInFdb(Transaction tr)
  {
    long now = System.currentTimeMillis();
    LockData lock = new LockData(_owner, now, 1);
    tr.set(_dataKey, lock.toBytes());
  }
  
  private void incrementHoldCountInFdb(Transaction tr, LockData lock)
  {
    long now = System.currentTimeMillis();
    lock.setLastLocked(now);
    lock.incrHoldCount();
    tr.set(_dataKey, lock.toBytes());
  }
  
  public boolean hasLock()
  {
    return hasLock(_fdb.createTransaction());
  }
  
  private boolean hasLock(Transaction tr)
  {
    LockData lock = getLock(tr);
    return hasLock(lock);
  }
  
  private boolean hasLock(LockData lock)
  {
    return lock != null && _owner.equals(lock.getOwner());
  }

  /**
   * Decrement hold count for the lock.
   * 
   * Caveat: this implementation does not restore the previous acquire time.
   * 
   * @return true if lock was successfully released, false otherwise
   *         (e.g., the owner does not have the lock)
   */
  public void release()
  {
    _fdb.run(new Function<Transaction, Void>()
    {
      public Void apply(Transaction tr)
      {
        if (hasLock(tr))
        {
          decrementHoldCountInFdb(tr);
          return null;
        }
        else
        {
          throw new IllegalMonitorStateException("Trying to release a lock not owned");
        }
      }
    });
  }
  
  private void decrementHoldCountInFdb(Transaction tr)
  {
    LockData lock = getLock(tr);
    if (lock.getHoldCount() <= 1)
    {
      deleteFromFdb(tr, lock);
    }
    else
    {
      long now = System.currentTimeMillis();
      lock.setLastLocked(now);
      lock.decrHoldCount();
      tr.set(_dataKey, lock.toBytes());
    }
  }

  /**
   * Delete this lock from FDB.
   */
  private void deleteFromFdb(Transaction tr, LockData lock)
  {
    tr.clear(_dataKey);
  }
  
  public int getHoldCount()
  {
    return _fdb.run(new Function<Transaction, Integer>()
    {
      @Override
      public Integer apply(Transaction tr)
      {
        LockData lock = getLock(tr);
        return hasLock(lock) ? lock.getHoldCount() : 0;
      }
    });
  }
  
  public long getLastAcquired()
  {
    return _fdb.run(new Function<Transaction, Long>()
    {
      @Override
      public Long apply(Transaction tr)
      {
        LockData lock = getLock(tr);
        return hasLock(lock) ? lock.getLastLocked() : 0;
      }
    });
  }
    
  private LockData getLock(Transaction tr)
  {
    byte[] bytes = tr.get(_dataKey).get();
    return bytes == null ? null : LockData.fromBytes(bytes);
  }
  
  private static class LockData
  {
    private static final int VERSION = 1;
    private static final int IDX_VERSION = 0;
    private static final int IDX_OWNER = 1;
    private static final int IDX_LAST_LOCKED = 2;
    private static final int IDX_HOLD_COUNT = 3;

    private final int _version;
    private final String _owner;
    private long _lastLocked;
    private int _holdCount;
    
    public LockData(String owner, long lastLocked, int holdCount)
    {
      this._version = VERSION;
      this._owner = owner;
      this._lastLocked = lastLocked;
      this._holdCount = holdCount;
    }
    
    @SuppressWarnings("unused")
    public int getVersion() { return _version; }
    public String getOwner() { return _owner; }
    public long getLastLocked() { return _lastLocked; }
    public void setLastLocked(long millis) { _lastLocked = millis; }
    public int getHoldCount() { return _holdCount; }
    public void incrHoldCount() { _holdCount++; }
    public void decrHoldCount() { _holdCount--; }
    
    public byte[] toBytes()
    {
      return Tuple.from(_version, _owner, _lastLocked, _holdCount).pack();
    }
    
    public static LockData fromBytes(byte[] bytes)
    {
      if (bytes == null)
      {
        return null;
      }

      Tuple tuple = Tuple.fromBytes(bytes);
      int version = (int) tuple.getLong(IDX_VERSION);
      if (version == VERSION)
      {
        String owner = tuple.getString(IDX_OWNER);
        long lastLocked = tuple.getLong(IDX_LAST_LOCKED);
        int holdCount = (int) tuple.getLong(IDX_HOLD_COUNT);
        return new LockData(owner, lastLocked, holdCount);
      }
      else
      {
        throw new IllegalArgumentException("Unsupported version: " + version);
      }
    }
  }

}
