package com.weikai77.fdb.util.concurrent;

import java.util.ArrayList;
import java.util.List;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;
import com.weikai77.fdb.util.ChangeMonitor;
import com.weikai77.fdb.util.ChangeMonitor.ChangeListener;
import com.weikai77.fdb.util.FdbUtils;
import com.weikai77.util.Clock;
import com.weikai77.util.SystemClock;

/**
 * Layout on FDB:
 * <pre>
 *    /members/{member_x}  -> {@link GroupMember}
 *            /{member_y}  -> {@link GroupMember}
 *            /...         -> {@link GroupMember}
 *    /watches/membership  -> [RANDOM BYTES]
 * </pre>
 * 
 * @author kwei
 *
 */
public class DistributedGroup
{
  private static final String KEY_WATCHES = "watches";
  private static final String KEY_MEMBERSHIP = "membership";
  private static final String KEY_MEMBERS = "members";

  private final Database _fdb;
  private final Subspace _space;
  private final String _id;
  private final long _ttl;
  private final ChangeMonitor _changeMonitor;
  private final Clock _clock;
  
  // derived and cached
  private final byte[] _membershipWatchKey;
  private final Subspace _membersSpace;

  protected DistributedGroup(Database fdb, Subspace space, String id, 
      long ttl, ChangeMonitor changeMonitor, Clock clock)
  {
    this._fdb = fdb;
    this._space = space;
    this._id = id;
    this._ttl = ttl;
    this._changeMonitor = changeMonitor;
    this._clock = clock;
    this._membershipWatchKey = _space.subspace(Tuple.from(KEY_WATCHES, KEY_MEMBERSHIP)).pack();
    this._membersSpace = _space.subspace(Tuple.from(KEY_MEMBERS));
  }
  
  protected DistributedGroup(Database fdb, Subspace space, String id, 
      long ttl, ChangeMonitor changeMonitor)
  {
    this(fdb, space, id, ttl, changeMonitor, SystemClock.getInstance());
  }

  public String getId()
  {
    return _id;
  }
  
  public List<String> listMembers()
  {
    return _fdb.run(new Function<Transaction,List<String>>()
    {
      @Override
      public List<String> apply(Transaction tr)
      {
        List<String> list = new ArrayList<>();
        tr.snapshot().getRange(_membersSpace.range()).forEach(kv -> 
        {
          // the last field of the key is the member id
          Tuple keyTuple = Tuple.fromBytes(kv.getKey());
          String memberId = keyTuple.getString(keyTuple.size()-1);
          
          // enforce TTL if any
          GroupMember member = GroupMember.fromBytes(kv.getValue());
          if (_ttl <= 0 || member.getTimestamp() + _ttl > _clock.currentTimeMillis())
          {
            list.add(memberId);
          }
        });
        
        return list;
      }
    });
  }
  
  public void join(String memberId)
  {
    _fdb.run(new Function<Transaction,Void>()
    {
      @Override
      public Void apply(Transaction tr)
      {
        tr.set(_membersSpace.pack(memberId), new GroupMember(_clock.currentTimeMillis()).toBytes());
        FdbUtils.signalWatch(tr, _membershipWatchKey);
        return null;
      }
    });
  }
  
  public void leave(String memberId)
  {
    _fdb.run(new Function<Transaction,Void>()
    {
      @Override
      public Void apply(Transaction tr)
      {
        tr.clear(_membersSpace.pack(memberId));
        FdbUtils.signalWatch(tr, _membershipWatchKey);
        return null;
      }
    });
  }
  
  public void registerMembershipListener(GroupMembershipListener listener)
  {
    _changeMonitor.registerListener(_membershipWatchKey, listener);
  }
  
  public void unregisterMembershipListener(GroupMembershipListener listener)
  {
    _changeMonitor.unregisterListener(_membershipWatchKey, listener);
  }
  
  private static class GroupMember
  {
    private static final int VERSION = 1;
    private static final int IDX_VERSION = 0;
    private static final int IDX_TIMESTAMP = 1;

    private final int _version;
    private final long _timestamp;
    
    public GroupMember(long timestamp)
    {
      this._version = VERSION;
      this._timestamp = timestamp;
    }
    
    public long getTimestamp()
    {
      return this._timestamp;
    }
    
    public byte[] toBytes()
    {
      return Tuple.from(_version, _timestamp).pack();
    }
    
    public static GroupMember fromBytes(byte[] bytes)
    {
      if (bytes == null)
      {
        return null;
      }
      
      Tuple tuple = Tuple.fromBytes(bytes);
      int version = (int) tuple.getLong(IDX_VERSION);
      if (version == VERSION)
      {
        long timestamp = tuple.getLong(IDX_TIMESTAMP);
        return new GroupMember(timestamp);
      }
      else
      {
        throw new IllegalArgumentException("Unsupported version: " + version);
      }
    }
  }

  public static abstract class GroupMembershipListener implements ChangeListener
  {
    @Override
    public void onChange(byte[] key)
    {
      onMembershipChange();
    }

    protected abstract void onMembershipChange();
  }
}
