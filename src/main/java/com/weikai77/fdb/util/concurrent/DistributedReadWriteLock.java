package com.weikai77.fdb.util.concurrent;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;
import com.weikai77.fdb.util.FdbUtils;

/**
 * A distributed read-write lock implementation on FDB.
 * <p>
 * This implementation is not reentrant.
 * <p>
 * Layout on FDB:
 * <pre>
 *    /data    -> {@link RwlData}
 *    /lock    -> {@link DistributedSimpleLock}
 * </pre>
 * 
 * @author kwei
 *
 */
public class DistributedReadWriteLock
{
  private static final String KEY_DATA = "data";
  private static final String KEY_LOCK = "lock";
  private static final String KEY_ZERO_WRITERS = "zeroWriters";
  private static final String KEY_ZERO_READER_WRITERS = "zeroReaderWriters";
  
  private final Database _fdb;
  private final Subspace _space;
  private final String _id;
  private final DistributedSimpleLock _lock;
  private final DistributedCondition _zeroWriters;
  private final DistributedCondition _zeroReaderWriters;

  // derived and cached
  private final byte[] _dataKey;
  
  protected DistributedReadWriteLock(Database fdb, Subspace space, ScheduledExecutorService timer,
      DistributedLockFactory lockFactory, String id)
  {
    _fdb = fdb;
    _space = space;
    _id = id;
    _lock = new DistributedSimpleLock(_fdb, _space.subspace(Tuple.from(KEY_LOCK)), timer, KEY_LOCK, 
        UUID.randomUUID().toString());
    _zeroWriters = _lock.newCondition(KEY_ZERO_WRITERS);
    _zeroReaderWriters = _lock.newCondition(KEY_ZERO_READER_WRITERS);
    _dataKey = _space.pack(KEY_DATA);
  }
  
  public String getId()
  {
    return _id;
  }
  
  public ReadLock readLock()
  {
    return new ReadLock();
  }
  
  public WriteLock writeLock()
  {
    return new WriteLock();
  }
  
  public int getReaderCount()
  {
    return getLockData().getReaderCount();
  }
  
  public int getWriterCount()
  {
    return getLockData().getWriterCount();
  }
  
  public int getWaitingReaderCount()
  {
    return getLockData().getWaitingReaderCount();
  }
  
  public int getWaitingWriterCount()
  {
    return getLockData().getWaitingWriterCount();
  }
  
  private RwlData getLockData()
  {
    RwlData data = RwlData.fromBytes(FdbUtils.get(_fdb, _dataKey));
    return data == null ? new RwlData() : data;
  }
  
  private RwlData updateLockData(java.util.function.Function<RwlData,RwlData> modifier)
  {
    return _fdb.run(new Function<Transaction, RwlData>()
    {
      @Override
      public RwlData apply(Transaction tr)
      {
        RwlData data = RwlData.fromBytes(tr.get(_dataKey).get());
        if (data == null)
        {
          data = new RwlData();
        }
        modifier.apply(data);
        tr.set(_dataKey, data.toBytes());
        return data;
      }
    });
  }

  /**
   * Increments the reader count and decrements the waiting reader count.
   */
  private RwlData onReaderAdmission()
  {
    return updateLockData(data -> data.incrReaderCount().decrWaitingReaderCount());
  }
  
  private RwlData decrReaderCount()
  {
    return updateLockData(data -> data.decrReaderCount());
  }
  
  /**
   * Increments the writer count and decrements the waiting writer count.
   */
  private RwlData onWriterAdmission()
  {
    return updateLockData(data -> data.incrWriterCount().decrWaitingWriterCount());
  }
  
  private RwlData decrWriterCount()
  {
    return updateLockData(data -> data.decrWriterCount());
  }
  
  private RwlData incrWaitingReaderCount()
  {
    return updateLockData(data -> data.incrWaitingReaderCount());
  }
  
  private RwlData decrWaitingReaderCount()
  {
    return updateLockData(data -> data.decrWaitingReaderCount());
  }
  
  private RwlData incrWaitingWriterCount()
  {
    return updateLockData(data -> data.incrWaitingWriterCount());
  }
  
  private RwlData decrWaitingWriterCount()
  {
    return updateLockData(data -> data.decrWaitingWriterCount());
  }
  
  private static class RwlData
  {
    private static final int VERSION = 1;
    private static final int IDX_VERSION = 0;
    private static final int IDX_READER_COUNT = 1;
    private static final int IDX_WRITER_COUNT = 2;
    private static final int IDX_WAITING_READER_COUNT = 3;
    private static final int IDX_WAITING_WRITER_COUNT = 4;

    private final int _version;
    private int _readerCount;
    private int _writerCount;
    private int _waitingReaderCount;
    private int _waitingWriterCount;
    
    public RwlData()
    {
      this(0, 0, 0, 0);
    }

    public RwlData(int readerCount, int writerCount,
                    int waitingReaderCount, int waitingWriterCount)
    {
      this._version = VERSION;
      this._readerCount = readerCount;
      this._writerCount = writerCount;
      this._waitingReaderCount = waitingReaderCount;
      this._waitingWriterCount = waitingWriterCount;
    }
    
    public int getReaderCount() { return _readerCount; }
    public int getWriterCount() { return _writerCount; }
    public int getWaitingReaderCount() { return _waitingReaderCount; }
    public int getWaitingWriterCount() { return _waitingWriterCount; }
    public RwlData incrReaderCount() { ++_readerCount; return this; }
    public RwlData decrReaderCount() { --_readerCount; return this; }
    public RwlData incrWriterCount() { ++_writerCount; return this; }
    public RwlData decrWriterCount() { --_writerCount; return this; }
    public RwlData incrWaitingReaderCount() { ++_waitingReaderCount; return this; }
    public RwlData decrWaitingReaderCount() { --_waitingReaderCount; return this; }
    public RwlData incrWaitingWriterCount() { ++_waitingWriterCount; return this; }
    public RwlData decrWaitingWriterCount() { --_waitingWriterCount; return this; }
    
    public byte[] toBytes()
    {
      return Tuple.from(_version, _readerCount, _writerCount, 
          _waitingReaderCount, _waitingWriterCount).pack();
    }
    
    public static RwlData fromBytes(byte[] bytes)
    {
      if (bytes == null)
      {
        return null;
      }
      
      Tuple tuple = Tuple.fromBytes(bytes);
      int version = (int) tuple.getLong(IDX_VERSION);
      if (version == VERSION)
      {
        int readerCount = (int) tuple.getLong(IDX_READER_COUNT);
        int writerCount = (int) tuple.getLong(IDX_WRITER_COUNT);
        int waitingReaderCount = (int) tuple.getLong(IDX_WAITING_READER_COUNT);
        int waitingWriterCount = (int) tuple.getLong(IDX_WAITING_WRITER_COUNT);
        return new RwlData(readerCount, writerCount, 
            waitingReaderCount, waitingWriterCount);
      }
      else
      {
        throw new IllegalArgumentException("Unsupported version: " + version);
      }
    }
  }
  
  public class ReadLock
  {
    public void lock() throws InterruptedException
    {
      _lock.acquire();
      try
      {
        incrWaitingReaderCount();

        boolean success = false;
        try
        {
          while (getLockData().getWriterCount() > 0)
          {
            _zeroWriters.await();
          }
          success = true;
        }
        finally
        {
          if (success)
          {
            onReaderAdmission();
          }
          else // some exception has occurred
          {
            decrWaitingReaderCount();
          }
        }
      }
      finally
      {
        _lock.release();
      }
    }
    
    public void unlock() throws InterruptedException
    {
      _lock.acquire();
      try
      {
        RwlData data = decrReaderCount();
        if (data.getReaderCount() <= 0 && data.getWriterCount() <= 0)
        {
          // no more readers or writers, good opportunity to clean up
          FdbUtils.clear(_fdb, _dataKey);

          _zeroReaderWriters.signalAll();
        }
      }
      finally
      {
        _lock.release();
      }
    }
  }
  
  public class WriteLock
  {
    public void lock() throws InterruptedException
    {
      _lock.acquire();
      try
      {
        incrWaitingWriterCount();

        boolean success = false;
        try
        {
          RwlData data = getLockData();
          while (data.getWriterCount() > 0 || data.getReaderCount() > 0)
          {
            _zeroReaderWriters.await();
            data = getLockData();
          }
          success = true;
        }
        finally
        {
          if (success)
          {
            onWriterAdmission();
          }
          else // some exception has occurred
          {
            decrWaitingWriterCount();
          }
        }
      }
      finally
      {
        _lock.release();
      }
    }
    
    public void unlock() throws InterruptedException
    {
      _lock.acquire();
      try
      {
        RwlData data = decrWriterCount();
        
        // TODO use the waiting reader/writer counts to determine
        //      whether to favor readers or writers
        
        // favor the writers
        if (data.getWriterCount() <= 0 && data.getReaderCount() <= 0)
        {
          // no more readers or writers, good opportunity to clean up
          FdbUtils.clear(_fdb, _dataKey);

          _zeroReaderWriters.signalAll();
        }
        
        if (data.getWriterCount() <= 0)
        {
          _zeroWriters.signalAll();
        }
      }
      finally
      {
        _lock.release();
      }
    }
  }

  /**
   * Just for fun, and reference implementation for distributed version
   * (See {@link DistributedReadWriteLock}).
   */
  public static class ReadWriteLock
  {
    private final Lock _lock;
    private final Condition _zeroWriters;
    private final Condition _zeroReaderWriters;
    private volatile int _readers;
    private volatile int _writers;
    
    public ReadWriteLock()
    {
      _lock = new ReentrantLock();
      _zeroWriters = _lock.newCondition();
      _zeroReaderWriters = _lock.newCondition();
    }
    
    public ReadLock readLock()
    {
      return new ReadLock();
    }
    
    public WriteLock writeLock()
    {
      return new WriteLock();
    }
    
    public class ReadLock
    {
      public void lock() throws InterruptedException
      {
        _lock.lock();
        try
        {
          while (_writers > 0)
          {
            _zeroWriters.await();
          }
          
          ++_readers;
        }
        finally
        {
          _lock.unlock();
        }
      }
      
      public void unlock()
      {
        _lock.lock();
        try
        {
          if (--_readers <= 0 && _writers <= 0)
          {
            _zeroReaderWriters.signal();
          }
        }
        finally
        {
          _lock.unlock();
        }
      }
    }
    
    public class WriteLock
    {
      public void lock() throws InterruptedException
      {
        _lock.lock();
        try
        {
          while (_writers > 0 || _readers > 0)
          {
            _zeroReaderWriters.await();
          }
          
          ++_writers;
        }
        finally
        {
          _lock.unlock();
        }
      }
      
      public void unlock()
      {
        _lock.lock();
        try
        {
          // favor the writers
          if (--_writers <= 0 && _readers <= 0)
          {
            _zeroReaderWriters.signal();
          }
          
          if (_writers <= 0)
          {
            _zeroWriters.signal();
          }
        }
        finally
        {
          _lock.unlock();
        }
      }
    }
  }

}


