package com.weikai77.fdb.util.concurrent;

import com.foundationdb.Database;
import com.foundationdb.KeySelector;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple;

/**
 * A distributed queue implementation on FDB.
 * <p>
 * Layout on FDB:
 * <pre>
 *    /items/0      -> {@link QueueItem}
 *          /1      -> {@link QueueItem}
 *          /...    -> {@link QueueItem}
 * </pre>
 * 
 * @author kwei
 *
 */
public class DistributedQueue implements Queue
{
  protected static final String KEY_ITEMS = "items";

  protected final Database _fdb;
  protected final Subspace _space;
  protected final String _id;

  // derived and cached
  protected final Subspace _itemsSpace;
  private final Range _itemsRange;
  
  protected DistributedQueue(Database db, Subspace space, String id)
  {
    _fdb = db;
    _space = space;
    _id = id;
    _itemsSpace = _space.subspace(Tuple.from(KEY_ITEMS));
    _itemsRange = _itemsSpace.range();
  }
  
  protected Subspace getSpace()
  {
    return _space;
  }
  
  @Override
  public String getId()
  {
    return _id;
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
  
  protected byte[] poll(Transaction tr)
  {
    byte[] headKey = getHeadKey(tr);
    if (headKey == null)
    {
      return null;
    }
    
    byte[] headValue = tr.get(headKey).get();
    QueueItem item = QueueItem.fromBytes(headValue);
    if (item == null)
    {
      return null;
    }

    tr.clear(headKey);
    return item.getValue();
  }
  
  private byte[] getHeadKey(Transaction tr)
  {
    KeySelector head = KeySelector.firstGreaterThan(_itemsRange.begin);
    byte[] headKey = tr.getKey(head).get();
    
    // check head falls within the range
    if (ByteArrayUtil.compareUnsigned(headKey, _itemsRange.end) > 0)
    {
      // the queue is empty
      return null;
    }
    else
    {
      return headKey;
    }
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
  
  protected boolean offer(Transaction tr, byte[] itemValue)
  {
    byte[] tailKey = getTailKey(tr);
    long newOffset = 0;

    if (tailKey != null)
    {
      // the queue is not empty
      byte[] tailValue = tr.get(tailKey).get();
      QueueItem item = QueueItem.fromBytes(tailValue);
      newOffset = item == null ? 0 : item.getOffset()+1;
    }

    QueueItem newItem = new QueueItem(newOffset, itemValue);
    byte[] newTailKey = _itemsSpace.pack(newOffset);
    tr.set(newTailKey, newItem.toBytes());
    return true;
  }
  
  private byte[] getTailKey(Transaction tr)
  {
    KeySelector tail = KeySelector.lastLessThan(_itemsRange.end);
    byte[] tailKey = tr.getKey(tail).get();

    // check tail falls within the range
    if (ByteArrayUtil.compareUnsigned(_itemsRange.begin, tailKey) <= 0)
    {
      // the queue is not empty
      return tailKey;
    }
    else
    {
      return null;
    }
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
  
  protected long size(Transaction tr)
  {
    byte[] headKey = getHeadKey(tr);
    if (headKey == null)
    {
      // the queue is empty
      return 0l;
    }
    
    // tail key cannot be null
    byte[] tailKey = getTailKey(tr);
    byte[] headValue = tr.get(headKey).get();
    byte[] tailValue = tr.get(tailKey).get();
    QueueItem headItem = QueueItem.fromBytes(headValue);
    QueueItem tailItem = QueueItem.fromBytes(tailValue);
    return tailItem.getOffset() - headItem.getOffset() + 1;
  }
  
  @Override
  public long capacity()
  {
    // unlimited capacity
    return 0;
  }

  @Override
  public boolean isEmpty()
  {
    return size() == 0;
  }
  
  @Override
  public boolean isFull()
  {
    return false;
  }
  
  protected boolean isFull(Transaction tr)
  {
    return false;
  }
  
  protected static class QueueItem
  {
    private static final int VERSION = 1;
    private static final int IDX_VERSION = 0;
    private static final int IDX_OFFSET = 1;
    private static final int IDX_VALUE = 2;

    private final int _version;
    private final long _offset;
    private final byte[] _value;
    
    public QueueItem(long offset, byte[] value)
    {
      this._version = VERSION;
      this._offset = offset;
      this._value = value;
    }
    
    public long getOffset() { return _offset; }
    public byte[] getValue() { return _value; }
    
    public byte[] toBytes()
    {
      return Tuple.from(_version, _offset, _value).pack();
    }
    
    public static QueueItem fromBytes(byte[] bytes)
    {
      if (bytes == null)
      {
        return null;
      }
      
      Tuple tuple = Tuple.fromBytes(bytes);
      int version = (int) tuple.getLong(IDX_VERSION);
      if (version == VERSION)
      {
        long offset = tuple.getLong(IDX_OFFSET);
        byte[] value = tuple.getBytes(IDX_VALUE);
        return new QueueItem(offset, value);
      }
      else
      {
        throw new IllegalArgumentException("Unsupported version: " + version);
      }
    }
  }

}
