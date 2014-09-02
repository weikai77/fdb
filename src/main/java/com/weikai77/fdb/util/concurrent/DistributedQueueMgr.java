package com.weikai77.fdb.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;
import com.weikai77.fdb.util.FdbUtils;
import com.weikai77.fdb.util.Space;

/**
 * Layout on FDB:
 * <pre>
 *    /meta/id   -> {@link QueueDef}
 *    /data/id   -> {@link DistributedQueue} | {@link DistributedBoundedQueue} | {@link DistributedBlockingQueue}
 * </pre>
 * 
 * @author kwei
 *
 */
public class DistributedQueueMgr implements DistributedQueueFactory
{
  private static final String KEY_META = "meta";
  private static final String KEY_DATA = "data";

  private final Database _fdb;
  private final Space _space;
  private final ScheduledExecutorService _timer;

  public DistributedQueueMgr(Database fdb, Space space, 
      DistributedLockMgr lockProvider)
  {
    this._fdb = fdb;
    this._space = space;
    this._timer = Executors.newSingleThreadScheduledExecutor();
  }
  
  private Subspace getMetaSpace(String id)
  {
    return _space.rawSubspace(KEY_META, id);
  }
  
  private Subspace getDataSpace(String id)
  {
    return _space.rawSubspace(KEY_DATA, id);
  }
  
  @Override
  public List<Queue> listQueues()
  {
    List<Queue> queues = new ArrayList<>();
    List<String> ids = FdbUtils.listChildrenAsStrings(_fdb, _space.rawSubspace(KEY_META));
    ids.forEach(id -> queues.add(getQueue(id)));
    return queues;
  }
  
  @Override
  public DistributedQueue createQueue(String id)
  {
    doCreateQueue(id, -1);
    return new DistributedQueue(_fdb, getDataSpace(id), id);
  }
  
  private QueueDef getQueueDef(Transaction tr, String id)
  {
    byte[] metaKey = getMetaSpace(id).pack();
    byte[] metaValue = tr.get(metaKey).get();
    return QueueDef.fromBytes(metaValue);
  }

  @Override
  public DistributedQueue createQueue(String id, long capacity)
  {
    doCreateQueue(id, capacity);
    return new DistributedBoundedQueue(_fdb, getDataSpace(id), id, capacity);
  }
  
  private void doCreateQueue(String id, long capacity)
  {
    _fdb.run(new Function<Transaction,Void>()
    {
      @Override
      public Void apply(Transaction tr)
      {
        QueueDef def = getQueueDef(tr, id);
        if (def != null)
        {
          throw new IllegalArgumentException("Queue already exists: " + id);
        }

        tr.set(getMetaSpace(id).pack(), new QueueDef(capacity).toBytes());
        return null;
      }
    });
  }

  @Override
  public DistributedQueue getQueue(String id)
  {
    QueueDef def = _fdb.run(new Function<Transaction,QueueDef>()
    {
      @Override
      public QueueDef apply(Transaction tr)
      {
        return getQueueDef(tr, id);
      }
    });
    
    if (def == null)
    {
      return null;
    }
    else if (def.isBounded())
    {
      return new DistributedBoundedQueue(_fdb, getDataSpace(id), id, def.getCapacity());
    }
    else
    {
      return new DistributedQueue(_fdb, getDataSpace(id), id);
    }
  }

  @Override
  public DistributedBlockingQueueLockFree createBlockingQueue(String id)
  {
    return new DistributedBlockingQueueLockFree(_fdb, createQueue(id), _timer);
  }
  
  public DistributedBlockingQueue createBlockingQueueWithLocking(String id)
  {
    return new DistributedBlockingQueue(_fdb, _timer, createQueue(id));
  }

  @Override
  public DistributedBlockingQueueLockFree createBlockingQueue(String id, long capacity)
  {
    return new DistributedBlockingQueueLockFree(_fdb, createQueue(id, capacity), _timer);
  }
  
  public DistributedBlockingQueue createBlockingQueueWithLocking(String id, long capacity)
  {
    return new DistributedBlockingQueue(_fdb, _timer, createQueue(id, capacity));
  }

  @Override
  public DistributedBlockingQueueLockFree getBlockingQueue(String id)
  {
    DistributedQueue queue = getQueue(id);
    if (queue == null)
    {
      return null;
    }
    else
    {
      return new DistributedBlockingQueueLockFree(_fdb, getQueue(id), _timer);
    }
  }

  public DistributedBlockingQueue getBlockingQueueWithLocking(String id)
  {
    DistributedQueue queue = getQueue(id);
    if (queue == null)
    {
      return null;
    }
    else
    {
      return new DistributedBlockingQueue(_fdb, _timer, getQueue(id));
    }
  }

  @Override
  public void deleteQueue(String id)
  {
    _fdb.run(new Function<Transaction,Void>()
    {
      @Override
      public Void apply(Transaction tr)
      {
        Subspace metaSpace = getMetaSpace(id);
        tr.clear(metaSpace.range());
        tr.clear(metaSpace.pack());
        
        Subspace dataSpace = getDataSpace(id);
        tr.clear(dataSpace.range());
        tr.clear(dataSpace.pack());
        return null;
      }
    });
  }

  private static class QueueDef
  {
    private static final int VERSION = 0;
    private static final int IDX_VERSION = 0;
    private static final int IDX_CAPACITY = 1;

    private final int _version;
    private final long _capacity;
    
    public QueueDef(long capacity)
    {
      this._version = VERSION;
      this._capacity = capacity;
    }
    
    public boolean isBounded() { return _capacity > 0; }
    public long getCapacity() { return _capacity; }
    
    public byte[] toBytes()
    {
      return Tuple.from(_version, _capacity).pack();
    }
    
    public static QueueDef fromBytes(byte[] bytes)
    {
      if (bytes == null)
      {
        return null;
      }
      
      Tuple tuple = Tuple.fromBytes(bytes);
      int version = (int) tuple.getLong(IDX_VERSION);
      if (version == VERSION)
      {
        long capacity = tuple.getLong(IDX_CAPACITY);
        return new QueueDef(capacity);
      }
      else
      {
        throw new IllegalArgumentException("Unsupported version:  " + version);
      }
    }
  }
  
}
