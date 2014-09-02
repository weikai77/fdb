package com.weikai77.fdb.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.foundationdb.Database;
import com.weikai77.fdb.util.DirectoryBasedSpace;
import com.weikai77.util.Clock;
import com.weikai77.util.SystemClock;

/**
 * Layout on FDB:
 * <pre>
 *   /locks/{id}        -> {@link DistributedSimpleLock}
 *   /reentrant/{id}    -> {@link DistributedReentrantLock}
 *   /conditions/{id}   -> {@link DistributedCondition}
 *   /rwl/{id}          -> {@link DistributedReadWriteLock}
 *   /countdown/{id}    -> {@link DistributedCountDownLatch}
 * </pre>
 * 
 * @author kwei
 *
 */
public class DistributedLockMgr implements DistributedLockFactory, DistributedConditionFactory
{
  private static final String PREFIX_LOCKS = "locks";
  private static final String PREFIX_REENTRANT = "reentrant";
  private static final String PREFIX_CONDITIONS = "conditions";
  private static final String PREFIX_READWRITELOCKS = "rwl";
  private static final String PREFIX_COUNTDOWN = "countdown";

  private final Database _fdb;
  private final DirectoryBasedSpace _space;
  private final DirectoryBasedSpace _locksSpace;
  private final DirectoryBasedSpace _reentrantSpace;
  private final DirectoryBasedSpace _conditionsSpace;
  private final DirectoryBasedSpace _rwlSpace;
  private final DirectoryBasedSpace _countDownSpace;
  private final ScheduledExecutorService _timer;
  private final Clock _clock;

  public DistributedLockMgr(Database fdb, DirectoryBasedSpace space)
  {
    this(fdb, space, SystemClock.getInstance());
  }

  public DistributedLockMgr(Database fdb, DirectoryBasedSpace space, Clock clock)
  {
    _fdb = fdb;
    _space = space;
    _locksSpace = _space.subspace(PREFIX_LOCKS);
    _reentrantSpace = _space.subspace(PREFIX_REENTRANT);
    _conditionsSpace = _space.subspace(PREFIX_CONDITIONS);
    _rwlSpace = _space.subspace(PREFIX_READWRITELOCKS);
    _countDownSpace = _space.subspace(PREFIX_COUNTDOWN);
    _timer = Executors.newSingleThreadScheduledExecutor();
    _clock = clock;
  }
 
  public List<DistributedSimpleLock> listSimpleLocks()
  {
    List<String> ids = _locksSpace.listChildren();
    List<DistributedSimpleLock> locks = new ArrayList<>(ids.size());
    for (String id : ids)
    {
      locks.add(newSimpleLock(id));
    }
    return locks;
  }

  @Override
  public DistributedSimpleLock newSimpleLock(String id)
  {
    String owner = UUID.randomUUID().toString();
    DistributedSimpleLock lock = new DistributedSimpleLock(_fdb, 
        _locksSpace.subspace(id).rawSubspace(), _timer, id, owner, _clock);
    return lock;
  }
  
  public void deleteSimpleLock(String id)
  {
    _locksSpace.delete(id);
  }

  public List<DistributedReentrantLock> listReentrantLocks()
  {
    List<String> ids = _reentrantSpace.listChildren();
    List<DistributedReentrantLock> locks = new ArrayList<>(ids.size());
    for (String id : ids)
    {
      locks.add(newReentrantLock(id));
    }
    return locks;
  }

  @Override
  public DistributedReentrantLock newReentrantLock(String id)
  {
    String owner = UUID.randomUUID().toString();
    DistributedReentrantLock lock = 
        new DistributedReentrantLock(_fdb, _reentrantSpace.subspace(id).rawSubspace(), _timer, id, owner);
    return lock;
  }
  
  public void deleteReentrantLock(String id)
  {
    _reentrantSpace.delete(id);
  }

  public List<DistributedReadWriteLock> listReadWriteLocks()
  {
    List<String> ids = _rwlSpace.listChildren();
    List<DistributedReadWriteLock> locks = new ArrayList<>(ids.size());
    for (String id : ids)
    {
      locks.add(newReadWriteLock(id));
    }
    return locks;
  }

  @Override
  public DistributedReadWriteLock newReadWriteLock(String id)
  {
    return new DistributedReadWriteLock(_fdb, _rwlSpace.subspace(id).rawSubspace(), _timer, this, id);
  }

  public void deleteReadWriteLock(String id)
  {
    _rwlSpace.delete(id);
  }

  public List<DistributedCondition> listConditions()
  {
    List<String> ids = _conditionsSpace.listChildren();
    List<DistributedCondition> result = new ArrayList<>(ids.size());
    for (String id : ids)
    {
      result.add(newCondition(id));
    }
    return result;
  }
  
  @Override
  public DistributedCondition newCondition(String id)
  {
    return new DistributedCondition(_fdb, _conditionsSpace.subspace(id).rawSubspace(), _timer, id, null);
  }

  public void deleteCondition(String id)
  {
    _conditionsSpace.delete(id);
  }
  
  public List<DistributedCountDownLatch> listCoundDownLatches()
  {
    List<String> ids = _countDownSpace.listChildren();
    List<DistributedCountDownLatch> result = new ArrayList<>(ids.size());
    for (String id : ids)
    {
      result.add(newCountDownLatch(id));
    }
    return result;
  }

  @Override
  public DistributedCountDownLatch newCountDownLatch(String id)
  {
    return new DistributedCountDownLatch(_fdb, _countDownSpace.subspace(id).rawSubspace(), _timer, id);
  }
  
  public void deleteCountDownLatch(String id)
  {
    _countDownSpace.delete(id);
  }

}
