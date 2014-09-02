package com.weikai77.fdb.util.concurrent;

import org.junit.Assert;
import org.junit.Test;

import com.weikai77.util.SettableClock;

/**
 * 
 * @author kwei
 *
 */
public class DistributedGroupTestIT
{
  @Test
  public void testSingleThreadedTupleBased() throws Exception
  {
    testSingleThreaded(TestUtils.getTupleBasedGroupMgr());
  }

  @Test
  public void testSingleThreadedDirectoryBased() throws Exception
  {
    testSingleThreaded(TestUtils.getDirectoryBasedGroupMgr());
  }

  private void testSingleThreaded(DistributedGroupMgr mgr) throws Exception
  {
    DistributedGroup group = mgr.createGroup("myTestGroup");
    try
    {
      Assert.assertTrue(group.listMembers().isEmpty());
      
      group.join("a");
      Assert.assertEquals(1, group.listMembers().size());
      group.join("b");
      Assert.assertEquals(2, group.listMembers().size());
      group.join("c");
      Assert.assertEquals(3, group.listMembers().size());
      group.leave("b");
      Assert.assertEquals(2, group.listMembers().size());
      group.join("d");
      Assert.assertEquals(3, group.listMembers().size());
      group.leave("c");
      Assert.assertEquals(2, group.listMembers().size());
      group.leave("a");
      Assert.assertEquals(1, group.listMembers().size());
      group.leave("a");
      Assert.assertEquals(1, group.listMembers().size());
      
      group = mgr.getGroup("myTestGroup");
      group.leave("d");
      Assert.assertEquals(0, group.listMembers().size());
      group.leave("d");
      Assert.assertEquals(0, group.listMembers().size());
      group.join("e");
      Assert.assertEquals(1, group.listMembers().size());
      group.join("e");
      Assert.assertEquals(1, group.listMembers().size());
      group.join("f");
      Assert.assertEquals(2, group.listMembers().size());
    }
    finally
    {
      mgr.deleteGroup("myTestGroup");
      Assert.assertTrue(mgr.listGroups().isEmpty());
    }
  }

  @Test
  public void testTtlTupleBased() throws Exception
  {
    SettableClock clock = new SettableClock(System.currentTimeMillis());
    testTtl(TestUtils.getTupleBasedGroupMgr(clock), clock);
  }

  @Test
  public void testTtlDirectoryBased() throws Exception
  {
    SettableClock clock = new SettableClock(System.currentTimeMillis());
    testTtl(TestUtils.getDirectoryBasedGroupMgr(clock), clock);
  }

  private void testTtl(DistributedGroupMgr mgr, SettableClock clock) throws Exception
  {
    DistributedGroup group = mgr.createGroup("myTestGroup", 60_000);
    try
    {
      Assert.assertTrue(group.listMembers().isEmpty());
      
      group.join("a");
      clock.tick(30_000);
      Assert.assertEquals(1, group.listMembers().size());
      group.join("b");
      clock.tick(20_000);
      Assert.assertEquals(2, group.listMembers().size());
      group.join("c");
      clock.tick(20_000);; // a expired
      Assert.assertEquals(2, group.listMembers().size());
      group.join("d");
      Assert.assertEquals(3, group.listMembers().size());
      group.leave("c");
      Assert.assertEquals(2, group.listMembers().size());
      group.leave("a");
      Assert.assertEquals(2, group.listMembers().size());
      group.leave("a");
      Assert.assertEquals(2, group.listMembers().size());

      clock.tick(30_000); // b expired
      group = mgr.getGroup("myTestGroup");
      Assert.assertEquals(1, group.listMembers().size());
      group.leave("d");
      Assert.assertEquals(0, group.listMembers().size());
      group.leave("d");
      Assert.assertEquals(0, group.listMembers().size());
      group.join("a");
      Assert.assertEquals(1, group.listMembers().size());
      group.join("e");
      Assert.assertEquals(2, group.listMembers().size());
      group.join("f");
      Assert.assertEquals(3, group.listMembers().size());
      
      clock.tick(60_000); // all expired
      Assert.assertEquals(0, group.listMembers().size());
      group.join("a");
      Assert.assertEquals(1, group.listMembers().size());
      group.join("b");
      Assert.assertEquals(2, group.listMembers().size());
      group.join("c");
      Assert.assertEquals(3, group.listMembers().size());
    }
    finally
    {
      mgr.deleteGroup("myTestGroup");
      Assert.assertTrue(mgr.listGroups().isEmpty());
    }
  }
}
