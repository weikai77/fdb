package com.weikai77.fdb.util.concurrent;

import java.util.ArrayList;
import java.util.List;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;
import com.weikai77.fdb.util.ChangeMonitor;
import com.weikai77.fdb.util.FdbUtils;
import com.weikai77.fdb.util.Space;
import com.weikai77.util.Clock;
import com.weikai77.util.SystemClock;

/**
 * 
 * Layout on FDB:
 * <pre>
 *    /meta/id    -> {@link GroupDef}
 *    /data/id    -> {@link DistributedGroup}
 * </pre>
 *
 * @author kwei
 *
 */
public class DistributedGroupMgr implements DistributedGroupFactory
{
  private static final String KEY_META = "meta";
  private static final String KEY_DATA = "data";

  private final Database _fdb;
  private final Space _space;
  private final ChangeMonitor _changeMonitor;
  private final Clock _clock;

  public DistributedGroupMgr(Database fdb, Space space)
  {
    this(fdb, space, SystemClock.getInstance());
  }

  public DistributedGroupMgr(Database fdb, Space space, Clock clock)
  {
    this._fdb = fdb;
    this._space = space;
    this._changeMonitor = new ChangeMonitor(_fdb);
    this._clock = clock;
  }

  /**
   * Lifecycle method.
   */
  public void shutdown()
  {
    _changeMonitor.shutdown();
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
  public List<DistributedGroup> listGroups()
  {
    List<DistributedGroup> groups = new ArrayList<>();
    List<String> ids = FdbUtils.listChildrenAsStrings(_fdb, _space.rawSubspace(KEY_META));
    ids.forEach(id -> groups.add(getGroup(id)));
    return groups;
  }
  
  @Override
  public DistributedGroup createGroup(String id)
  {
    return createGroup(id, 0l);
  }
  
  @Override
  public DistributedGroup createGroup(String id, long ttl)
  {
    doCreateGroup(id, ttl);
    return new DistributedGroup(_fdb, getDataSpace(id), id, ttl, _changeMonitor, _clock);
  }

  private GroupDef getGroupDef(Transaction tr, String id)
  {
    byte[] metaKey = getMetaSpace(id).pack();
    byte[] metaValue = tr.get(metaKey).get();
    return GroupDef.fromBytes(metaValue);
  }

  private void doCreateGroup(String id, long ttl)
  {
    _fdb.run(new Function<Transaction,Void>()
    {
      @Override
      public Void apply(Transaction tr)
      {
        GroupDef def = getGroupDef(tr, id);
        if (def != null)
        {
          throw new IllegalArgumentException("Group already exists: " + id);
        }

        tr.set(getMetaSpace(id).pack(), new GroupDef(ttl).toBytes());
        return null;
      }
    });
  }

  @Override
  public DistributedGroup getGroup(String id)
  {
    GroupDef def = _fdb.run(new Function<Transaction,GroupDef>()
    {
      @Override
      public GroupDef apply(Transaction tr)
      {
        return getGroupDef(tr, id);
      }
    });
    
    if (def == null)
    {
      return null;
    }
    else
    {
      return new DistributedGroup(_fdb, getDataSpace(id), id, 
          def.getTtl(), _changeMonitor, _clock);
    }
  }

  @Override
  public void deleteGroup(String id)
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
  
  private static class GroupDef
  {
    private static final int VERSION = 0;
    private static final int IDX_VERSION = 0;
    private static final int IDX_TTL = 1;

    private final int _version;
    private final long _ttl;
    
    public GroupDef(long ttl)
    {
      this._version = VERSION;
      this._ttl = ttl;
    }
    
    public long getTtl()
    {
      return _ttl;
    }
    
    public byte[] toBytes()
    {
      return Tuple.from(_version, _ttl).pack();
    }
    
    public static GroupDef fromBytes(byte[] bytes)
    {
      if (bytes == null)
      {
        return null;
      }
      
      Tuple tuple = Tuple.fromBytes(bytes);
      int version = (int) tuple.getLong(IDX_VERSION);
      if (version == VERSION)
      {
        long ttl = tuple.getLong(IDX_TTL);
        return new GroupDef(ttl);
      }
      else
      {
        throw new IllegalArgumentException("Unsupported version:  " + version);
      }
    }
  }
}
