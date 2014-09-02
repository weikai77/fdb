package com.weikai77.fdb.util.concurrent;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.weikai77.fdb.util.DirectoryBasedSpace;
import com.weikai77.fdb.util.TupleBasedSpace;
import com.weikai77.util.Clock;
import com.weikai77.util.SystemClock;

/**
 * 
 * @author kwei
 *
 */
public class TestUtils
{
  public static DistributedLockMgr getLockMgr()
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();
    return new DistributedLockMgr(db, new DirectoryBasedSpace(db, "test", "locks"));
  }

  public static DistributedLockMgr getLockMgr(Clock clock)
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();
    return new DistributedLockMgr(db, new DirectoryBasedSpace(db, "test", "locks"), clock);
  }

  public static DistributedQueueMgr getTupleBasedQueueMgr()
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();
    return new DistributedQueueMgr(db, new TupleBasedSpace("test", "queues"), getLockMgr());
  }
  
  public static DistributedQueueMgr getDirectoryBasedQueueMgr()
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();
    return new DistributedQueueMgr(db, new DirectoryBasedSpace(db, "test", "queues"),
        getLockMgr());
  }

  public static DistributedGroupMgr getTupleBasedGroupMgr()
  {
    return getTupleBasedGroupMgr(SystemClock.getInstance());
  }

  public static DistributedGroupMgr getTupleBasedGroupMgr(Clock clock)
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();
    return new DistributedGroupMgr(db, new TupleBasedSpace("test", "groups"), clock);
  }
  
  public static DistributedGroupMgr getDirectoryBasedGroupMgr()
  {
    return getDirectoryBasedGroupMgr(SystemClock.getInstance());
  }

  public static DistributedGroupMgr getDirectoryBasedGroupMgr(Clock clock)
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();
    return new DistributedGroupMgr(db, new DirectoryBasedSpace(db, "test", "groups"), clock);
  }

}
