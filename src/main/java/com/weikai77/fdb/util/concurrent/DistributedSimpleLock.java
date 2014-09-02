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
import com.weikai77.util.Clock;
import com.weikai77.util.SystemClock;

/**
 * A distributed, non-reentrant lock implementation on FDB.
 * <p>
 * Note that we need an external process to fully enforce the TTL's.
 * Without that external process to periodically release expired 
 * locks, threads calling the blocking acquire() method may get
 * blocked indefinitely even after the lock becomes available due
 * to the prior holder expiring.
 * <p>
 * Layout on FDB:
 * <pre>
 *    /data         -> {@link LockData}
 *    /conditions/  -> {@link DistributedCondition}
 * </pre>
 * 
 * @author kwei
 *
 */
public class DistributedSimpleLock
{
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedSimpleLock.class);

  private static final String KEY_DATA = "data";
  private static final String KEY_CONDITIONS = "conditions";

  private final Database _fdb;
  private final Subspace _space;
  private final ScheduledExecutorService _timer;
  private final String _id;
  private final String _owner;
  private final Clock _clock;

  // derived and cached
  private final byte[] _dataKey;
  
  protected DistributedSimpleLock(Database db, Subspace space, ScheduledExecutorService timer, 
      String id, String owner, Clock clock)
  {
    this._fdb = db;
    this._space = space;
    this._timer = timer;
    this._id = id;
    this._owner = owner;
    this._clock = clock;
    this._dataKey = _space.pack(Tuple.from(KEY_DATA));
  }

  protected DistributedSimpleLock(Database db, Subspace space, ScheduledExecutorService timer, 
      String id, String owner)
  {
    this(db, space, timer, id, owner, SystemClock.getInstance());
  }

  public DistributedCondition newCondition(String id)
  {
    return new DistributedCondition(_fdb, _space.subspace(Tuple.from(KEY_CONDITIONS, id)), 
        _timer, id, this);
  }
  
  public String getId()
  {
    return _id;
  }
  
  public boolean tryAcquire()
  {
    return acquireIfAvailable(0);
  }
  
  public boolean tryAcquire(long timeoutMillis) throws InterruptedException
  {
    return tryAcquire(timeoutMillis, 0);
  }

  public boolean tryAcquire(long timeoutMillis, long ttlMillis) throws InterruptedException
  {
    long startTime = System.currentTimeMillis();
    while (true)
    {
      Watch watch = _fdb.run(new Function<Transaction,Watch>()
      {
        @Override
        public Watch apply(Transaction tr)
        {
          if (acquireIfAvailable(tr, ttlMillis))
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
    acquire(0);
  }

  public void acquire(long ttlMillis)
  {
    while (true)
    {
      Watch watch = _fdb.run(new Function<Transaction,Watch>()
      {
        @Override
        public Watch apply(Transaction tr)
        {
          if (acquireIfAvailable(tr, ttlMillis))
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
  
  private boolean acquireIfAvailable(long ttlMillis)
  {
    return _fdb.run(new Function<Transaction, Boolean>()
    {
      public Boolean apply(Transaction tr)
      {
        return acquireIfAvailable(tr, ttlMillis);
      }
    });
  }
  
  private boolean acquireIfAvailable(Transaction tr, long ttlMillis)
  {
    LockData lock = getLock(tr);
    if (lock == null || lock.hasExpired(_clock))
    {
      // lock is free
      createLockInFdb(tr, ttlMillis);
      return true;
    }
    else if (_owner.equals(lock.getOwner()))
    {
      // already locked by me
      renewLockInFdb(tr, lock, ttlMillis);
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
  private void createLockInFdb(Transaction tr, long ttlMillis)
  {
    long now = _clock.currentTimeMillis();
    LockData lock = new LockData(_owner, now, now, ttlMillis > 0 ? now+ttlMillis : 0);
    tr.set(_dataKey, lock.toBytes());
  }
  
  private void renewLockInFdb(Transaction tr, LockData lock, long ttlMillis)
  {
    long now = _clock.currentTimeMillis();
    lock.setLastLockTime(now);
    lock.setExpireTime(ttlMillis > 0 ? now+ttlMillis : 0);;
    tr.set(_dataKey, lock.toBytes());
  }
  
  public boolean hasLock()
  {
    return _fdb.run(new Function<Transaction,Boolean>()
    {
      @Override
      public Boolean apply(Transaction tr)
      {
        return hasLock(tr);
      }
    });
  }
  
  private boolean hasLock(Transaction tr)
  {
    LockData lock = getLock(tr);
    return hasLock(lock);
  }
  
  private boolean hasLock(LockData lock)
  {
    return lock != null && _owner.equals(lock.getOwner()) 
        && !lock.hasExpired(_clock);
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
        LockData lock = getLock(tr);
        if (hasLock(lock))
        {
          deleteFromFdb(tr, lock);
          return null;
        }
        else
        {
          throw new IllegalMonitorStateException("Trying to release a lock not owned");
        }
      }
    });
  }
  
  public long getInitialLockTime()
  {
    return _fdb.run(new Function<Transaction, Long>()
    {
      public Long apply(Transaction tr)
      {
        LockData lock = getLock(tr);
        if (hasLock(lock))
        {
          return lock.getInitialLockTime();
        }
        else
        {
          return 0l;
        }
      }
    });
  }
  
  public long getLastLockTime()
  {
    return _fdb.run(new Function<Transaction, Long>()
    {
      public Long apply(Transaction tr)
      {
        LockData lock = getLock(tr);
        if (hasLock(lock))
        {
          return lock.getLastLockTime();
        }
        else
        {
          return 0l;
        }
      }
    });
  }
  
  public boolean hasExpired()
  {
    return _fdb.run(new Function<Transaction, Boolean>()
    {
      public Boolean apply(Transaction tr)
      {
        LockData lock = getLock(tr);
        return lock != null && lock.hasExpired(_clock);
      }
    });
  }
  
  /**
   * Delete this lock from FDB.
   */
  private void deleteFromFdb(Transaction tr, LockData lock)
  {
    tr.clear(_dataKey);
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
    private static final int IDX_INITIAL_LOCK_TIME = 2;
    private static final int IDX_LAST_LOCK_TIME = 3;
    private static final int IDX_EXPIRE_TIME = 4;

    private final int _version;
    private final String _owner;
    private long _initialLockTime;
    private long _lastLockTime;
    private long _expireTime;
    
    public LockData(String owner, long initialLockTime, long lastLockTime, long expireTime)
    {
      this._version = VERSION;
      this._owner = owner;
      this._initialLockTime = initialLockTime;
      this._lastLockTime = lastLockTime;
      this._expireTime = expireTime;
    }
    
    public String getOwner() { return _owner; }
    public long getInitialLockTime() { return _initialLockTime; }
    public long getLastLockTime() { return _lastLockTime; }
    public void setLastLockTime(long millis) { _lastLockTime = millis; }
    public void setExpireTime(long millis) { _expireTime = millis; }
    public boolean hasExpired(Clock clock) { return _expireTime > 0 && _expireTime < clock.currentTimeMillis(); }
    
    public byte[] toBytes()
    {
      return Tuple.from(_version, _owner, _initialLockTime, _lastLockTime, _expireTime).pack();
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
        long initialLockTime = tuple.getLong(IDX_INITIAL_LOCK_TIME);
        long lastLockTime = tuple.getLong(IDX_LAST_LOCK_TIME);
        long expireTime = tuple.getLong(IDX_EXPIRE_TIME);
        return new LockData(owner, initialLockTime, lastLockTime, expireTime);
      }
      else
      {
        throw new IllegalArgumentException("Unsupported version: " + version);
      }
    }
  }

}
