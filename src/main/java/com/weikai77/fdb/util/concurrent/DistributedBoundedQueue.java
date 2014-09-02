package com.weikai77.fdb.util.concurrent;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;

/**
 * An alternative distributed queue implementation on FDB.
 * <p>
 * Layout on FDB:
 * <pre>
 *    /meta         -> {@link QueueMeta}
 *    /items/0      -> {@link QueueItem} 
 *          /1      -> {@link QueueItem} 
 *          /...    -> {@link QueueItem} 
 * </pre>
 * 
 * @author kwei
 *
 */
public class DistributedBoundedQueue extends DistributedQueue
{
  private static final String KEY_META = "meta";

  private final long _capacity;
  
  // derived and cached
  private final byte[] _metaKey;
  
  protected DistributedBoundedQueue(Database db, Subspace space, String id, long capacity)
  {
    super(db, space, id);

    _capacity = capacity;
    _metaKey = _space.subspace(Tuple.from(KEY_META)).pack();
  }
  
  @Override
  public byte[] poll()
  {
    return _fdb.run(new Function<Transaction,byte[]>()
    {
      @Override
      public byte[] apply(Transaction tr)
      {
        return poll(tr);
      }
    });
  }
  
  @Override
  protected byte[] poll(Transaction tr)
  {
    QueueMeta meta = getQueueMeta(tr);
    if (meta == null || meta.isEmpty())
    {
      return null;
    }
    
    byte[] headKey = getItemKey(meta.getHeadOffset());
    byte[] headValue = tr.get(headKey).get();
    QueueItem item = QueueItem.fromBytes(headValue);
    tr.clear(headKey);
    meta.advanceHead();
    tr.set(_metaKey, meta.toBytes());
    return item.getValue();
  }
  
  @Override
  public boolean offer(byte[] itemValue)
  {
    return _fdb.run(new Function<Transaction,Boolean>()
    {
      @Override
      public Boolean apply(Transaction tr)
      {
        return offer(tr, itemValue);
      }
    });
  }

  @Override
  protected boolean offer(Transaction tr, byte[] itemValue)
  {
    QueueMeta meta = getQueueMeta(tr);
    if (meta == null)
    {
      meta = new QueueMeta(_capacity);
    }

    if (meta.isFull())
    {
      return false;
    }

    byte[] tailKey = getItemKey(meta.getTailOffset());
    QueueItem newItem = new QueueItem(meta.getTailOffset(), itemValue);
    tr.set(tailKey, newItem.toBytes());
    meta.advanceTail();
    tr.set(_metaKey, meta.toBytes());
    return true;
  }
  
  private byte[] getItemKey(long offset)
  {
    return _itemsSpace.pack(offset);
  }
  
  @Override
  public long size()
  {
    return _fdb.run(new Function<Transaction,Long>()
    {
      @Override
      public Long apply(Transaction tr)
      {
        return size(tr);
      }
    });
  }
  
  @Override
  protected long size(Transaction tr)
  {
    QueueMeta meta = getQueueMeta(tr);
    if (meta == null)
    {
      return 0l;
    }
    else
    {
      return meta.size();
    }
  }
  
  @Override
  public long capacity()
  {
    return _capacity;
  }
  
  @Override
  public boolean isEmpty()
  {
    return size() == 0;
  }
  
  @Override
  public boolean isFull()
  {
    return _fdb.run(new Function<Transaction,Boolean>()
    {
      @Override
      public Boolean apply(Transaction tr)
      {
        return isFull(tr);
      }
    });
  }
  
  @Override
  protected boolean isFull(Transaction tr)
  {
    QueueMeta meta = getQueueMeta(tr);
    if (meta == null)
    {
      return false;
    }
    else
    {
      return meta.isFull();
    }
  }

  private QueueMeta getQueueMeta(Transaction tr)
  {
    byte[] bytes = tr.get(_metaKey).get();
    return QueueMeta.fromBytes(bytes);
  }
  
  protected static class QueueMeta
  {
    private static final int VERSION = 1;
    private static final int IDX_VERSION = 0;
    private static final int IDX_CAPCITY = 1;
    private static final int IDX_HEAD_OFFSET = 2;
    private static final int IDX_SIZE = 3;

    private int _version;
    private long _capacity;
    private long _headOffset;
    private long _size;

    public QueueMeta(long capacity)
    {
      this(capacity, 0, 0);
    }

    public QueueMeta(long capacity, long headOffset, long size)
    {
      this._version = VERSION;
      this._capacity = capacity;
      this._headOffset = headOffset;
      this._size = size;
    }
    
    public long getHeadOffset() { return _headOffset; }
    public long getTailOffset() { return (_headOffset + _size) % _capacity; }
    public void advanceHead() { _headOffset = (_headOffset + 1) % _capacity; --_size; }
    public void advanceTail() { ++_size; }
    public long size() { return _size; }
    public boolean isEmpty() { return _size == 0; }
    public boolean isFull() { return _size >= _capacity; }
    
    public byte[] toBytes()
    {
      return Tuple.from(_version, _capacity, _headOffset, _size).pack();
    }

    public static QueueMeta fromBytes(byte[] bytes)
    {
      if (bytes == null)
      {
        return null;
      }
      
      Tuple tuple = Tuple.fromBytes(bytes);
      int version = (int) tuple.getLong(IDX_VERSION);
      if (version == VERSION)
      {
        long capacity = tuple.getLong(IDX_CAPCITY);
        long headOffset = tuple.getLong(IDX_HEAD_OFFSET);
        long size = tuple.getLong(IDX_SIZE);
        return new QueueMeta(capacity, headOffset, size);
      }
      else
      {
        throw new IllegalArgumentException("Unsupported version: " + version);
      }
    }
  }
  
}
