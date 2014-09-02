package com.weikai77.fdb.util.concurrent;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author kwei
 *
 */
public class DistributedGroupMgrTestIT 
{
  @Test
  public void testCRUDTupleBased() throws Exception
  {
    testCRUD(TestUtils.getTupleBasedGroupMgr());
  }

  @Test
  public void testCRUDDirectoryBased() throws Exception
  {
    testCRUD(TestUtils.getDirectoryBasedGroupMgr());
  }
  
  private void testCRUD(DistributedGroupMgr mgr) throws Exception
  {
    Assert.assertTrue(mgr.listGroups().isEmpty());
    try
    {
      List<DistributedGroup> groups = null;

      // Group 1
      DistributedGroup group1 = mgr.createGroup("group1");
      Assert.assertEquals("group1", group1.getId());

      group1 = mgr.getGroup("group1");
      Assert.assertNotNull(group1);
      Assert.assertEquals("group1", group1.getId());

      groups = mgr.listGroups();
      Assert.assertEquals(1, groups.size());
      group1 = groups.get(0);
      Assert.assertNotNull(group1);
      Assert.assertEquals("group1", group1.getId());

      try
      {
        mgr.createGroup("group1");
        Assert.fail("Should have failed but did not");
      }
      catch (IllegalArgumentException ex) {}

      // Group 2
      DistributedGroup group2 = mgr.createGroup("group2");
      Assert.assertEquals("group2", group2.getId());

      group2 = mgr.getGroup("group2");
      Assert.assertNotNull(group2);
      Assert.assertEquals("group2", group2.getId());

      groups = mgr.listGroups();
      Assert.assertEquals(2, groups.size());
      group1 = groups.get(0);
      Assert.assertNotNull(group1);
      Assert.assertEquals("group1", group1.getId());
      group2 = groups.get(1);
      Assert.assertNotNull(group2);
      Assert.assertEquals("group2", group2.getId());

      // delete group 1
      mgr.deleteGroup("group1");
      Assert.assertEquals(1, mgr.listGroups().size());
      Assert.assertNull(mgr.getGroup("group1"));

      // recreate group 1
      group1 = mgr.createGroup("group1");
      Assert.assertEquals("group1", group1.getId());

      group1 = mgr.getGroup("group1");
      Assert.assertNotNull(group1);
      Assert.assertEquals("group1", group1.getId());

      groups = mgr.listGroups();
      Assert.assertEquals(2, groups.size());
      group1 = groups.get(0);
      Assert.assertNotNull(group1);
      Assert.assertEquals("group1", group1.getId());
      group2 = groups.get(1);
      Assert.assertNotNull(group2);
      Assert.assertEquals("group2", group2.getId());
    }
    finally
    {
      mgr.deleteGroup("group1");
      mgr.deleteGroup("group2");
      Assert.assertTrue(mgr.listGroups().isEmpty());
    }
  }
  
}
