package com.weikai77.fdb.util.concurrent;

import org.junit.Assert;
import org.junit.Test;

import com.weikai77.util.SettableClock;

/**
 * 
 * @author kwei
 *
 */
public class GroupKeyMapperTestIT
{
  @Test
  public void testReplication1TupleBased()
  {
    SettableClock clock = new SettableClock(System.currentTimeMillis());
    testReplication1(TestUtils.getTupleBasedGroupMgr(clock), clock);
  }

  @Test
  public void testReplication1DirectoryBased()
  {
    SettableClock clock = new SettableClock(System.currentTimeMillis());
    testReplication1(TestUtils.getDirectoryBasedGroupMgr(clock), clock);
  }

  private void testReplication1(DistributedGroupMgr mgr, SettableClock clock)
  {
    DistributedGroup group = mgr.createGroup("myTestGroup", 60_000);
    
    try
    {
      GroupKeyMapper mapper = new GroupKeyMapper(mgr.getGroup("myTestGroup"), 1, 500l);
      mapper.start();
      try
      {
        Assert.assertNull(mapper.map("key1"));
        Assert.assertNull(mapper.map("key2"));
    
        /**
         * (node1, 0) ->  -353732108
         * (key1, 0)  ->   133378825
         * (key2, 0)  ->  2072764077
         */
        group.join("node1");
        while (mapper.size() == 0) {}
        Assert.assertEquals("node1", mapper.map("key1"));
        Assert.assertEquals("node1", mapper.map("key2"));
    
        /**
         * (node2, 0) -> -1074218907
         * (node1, 0) ->  -353732108
         * (key1, 0)  ->   133378825
         * (key2, 0)  ->  2072764077
         */
        clock.tick(10_000);
        group.join("node2");
        while (mapper.size() == 1) {}
        Assert.assertEquals("node2", mapper.map("key1"));
        Assert.assertEquals("node2", mapper.map("key2"));
    
        /**
         * (node3, 0) -> -1796370422
         * (node2, 0) -> -1074218907
         * (node1, 0) ->  -353732108
         * (key1, 0)  ->   133378825
         * (key2, 0)  ->  2072764077
         */
        clock.tick(10_000);
        group.join("node3");
        while (mapper.size() == 2) {}
        Assert.assertEquals("node3", mapper.map("key1"));
        Assert.assertEquals("node3", mapper.map("key2"));
    
        /**
         * (node3, 0) -> -1796370422
         * (node4, 0) -> -1322416027
         * (node2, 0) -> -1074218907
         * (node1, 0) ->  -353732108
         * (key1, 0)  ->   133378825
         * (key2, 0)  ->  2072764077
         */
        clock.tick(10_000);
        group.join("node4");
        while (mapper.size() == 3) {}
        Assert.assertEquals("node3", mapper.map("key1"));
        Assert.assertEquals("node3", mapper.map("key2"));
    
        /**
         * (node3, 0) -> -1796370422
         * (node4, 0) -> -1322416027
         * (node2, 0) -> -1074218907
         * (node5, 0) ->  -454886638
         * (node1, 0) ->  -353732108
         * (key1, 0)  ->   133378825
         * (key2, 0)  ->  2072764077
         */
        clock.tick(10_000);
        group.join("node5");
        while (mapper.size() == 4) {}
        Assert.assertEquals("node3", mapper.map("key1"));
        Assert.assertEquals("node3", mapper.map("key2"));
    
        /**
         * (node3, 0) -> -1796370422
         * (node4, 0) -> -1322416027
         * (node2, 0) -> -1074218907
         * (node5, 0) ->  -454886638
         * (node1, 0) ->  -353732108
         * (key1, 0)  ->   133378825
         * (node6, 0) ->  1151568728
         * (key2, 0)  ->  2072764077
         */
        clock.tick(10_000);
        group.join("node6");
        while (mapper.size() == 5) {}
        Assert.assertEquals("node6", mapper.map("key1"));
        Assert.assertEquals("node3", mapper.map("key2"));
        /**
         * (node3, 0) -> -1796370422
         * (node4, 0) -> -1322416027
         * (node2, 0) -> -1074218907
         * (node5, 0) ->  -454886638
         * (key1, 0)  ->   133378825
         * (node6, 0) ->  1151568728
         * (key2, 0)  ->  2072764077
         */
        group.leave("node1");
        while (mapper.size() == 6) {}
        Assert.assertEquals("node6", mapper.map("key1"));
        Assert.assertEquals("node3", mapper.map("key2"));
        
        /**
         * (node3, 0) -> -1796370422
         * (node4, 0) -> -1322416027
         * (node5, 0) ->  -454886638
         * (key1, 0)  ->   133378825
         * (node6, 0) ->  1151568728
         * (key2, 0)  ->  2072764077
         */
        clock.tick(20_000); // node2 expired
        while (mapper.size() == 5) {}
        Assert.assertEquals("node6", mapper.map("key1"));
        Assert.assertEquals("node3", mapper.map("key2"));
        
        /**
         * (node4, 0) -> -1322416027
         * (node5, 0) ->  -454886638
         * (key1, 0)  ->   133378825
         * (node6, 0) ->  1151568728
         * (key2, 0)  ->  2072764077
         */
        group.leave("node3");
        while (mapper.size() == 4) {}
        Assert.assertEquals("node6", mapper.map("key1"));
        Assert.assertEquals("node4", mapper.map("key2"));
        
        /**
         * (node5, 0) ->  -454886638
         * (key1, 0)  ->   133378825
         * (node6, 0) ->  1151568728
         * (key2, 0)  ->  2072764077
         */
        clock.tick(20_000); // node4 expired
        while (mapper.size() == 3) {}
        Assert.assertEquals("node6", mapper.map("key1"));
        Assert.assertEquals("node5", mapper.map("key2"));
        
        /**
         * (key1, 0)  ->   133378825
         * (node6, 0) ->  1151568728
         * (key2, 0)  ->  2072764077
         */
        group.leave("node5");
        while (mapper.size() == 2) {}
        Assert.assertEquals("node6", mapper.map("key1"));
        Assert.assertEquals("node6", mapper.map("key2"));

        /**
         * (key1, 0)  ->   133378825
         * (key2, 0)  ->  2072764077
         */
        clock.tick(20_000); // node6 expired
        while (mapper.size() == 1) {}
        Assert.assertNull(mapper.map("key1"));
        Assert.assertNull(mapper.map("key2"));
      }
      finally
      {
        mapper.shutdown();
      }
    }
    finally
    {
      mgr.deleteGroup("myTestGroup");
    }
  }

  @Test
  public void testReplication3TupleBased()
  {
    SettableClock clock = new SettableClock(System.currentTimeMillis());
    testReplication3(TestUtils.getTupleBasedGroupMgr(clock), clock);
  }

  @Test
  public void testReplication3DirectoryBased()
  {
    SettableClock clock = new SettableClock(System.currentTimeMillis());
    testReplication3(TestUtils.getDirectoryBasedGroupMgr(clock), clock);
  }

  private void testReplication3(DistributedGroupMgr mgr, SettableClock clock)
  {
    DistributedGroup group = mgr.createGroup("myTestGroup", 60_000);
    
    try
    {
      GroupKeyMapper mapper = new GroupKeyMapper(mgr.getGroup("myTestGroup"), 3, 500l);
      mapper.start();
      try
      {
        Assert.assertNull(mapper.map("key1"));
        Assert.assertNull(mapper.map("key2"));
    
        /**
         * (node1, 0) ->  -353732108
         * (key1, 0)  ->   133378825
         * (node1, 1) ->   512294370
         * (key2, 0)  ->  2072764077
         * (node1, 2) ->  2094418500
         */
        group.join("node1");
        while (mapper.size() == 0) {}
        Assert.assertEquals("node1", mapper.map("key1"));
        Assert.assertEquals("node1", mapper.map("key2"));
        
        /**
         * (node2, 0) -> -1074218907
         * (node1, 0) ->  -353732108
         * (node2, 1) ->  -102852506
         * (key1, 0)  ->   133378825
         * (node1, 1) ->   512294370
         * (node2, 2) ->  1968027681
         * (key2, 0)  ->  2072764077
         * (node1, 2) ->  2094418500
         */
        clock.tick(10_000);
        group.join("node2");
        while (mapper.size() == 3) {}
        Assert.assertEquals("node1", mapper.map("key1"));
        Assert.assertEquals("node1", mapper.map("key2"));
        
        /**
         * (node3, 0) -> -1796370422
         * (node2, 0) -> -1074218907
         * (node1, 0) ->  -353732108
         * (node2, 1) ->  -102852506
         * (key1, 0)  ->   133378825
         * (node3, 1) ->   350282656
         * (node1, 1) ->   512294370
         * (node3, 2) ->  1809479839
         * (node2, 2) ->  1968027681
         * (key2, 0)  ->  2072764077
         * (node1, 2) ->  2094418500
         */
        clock.tick(10_000);
        group.join("node3");
        while (mapper.size() == 6) {}
        Assert.assertEquals("node3", mapper.map("key1"));
        Assert.assertEquals("node1", mapper.map("key2"));
        
        /**
         * (node3, 0) -> -1796370422
         * (node4, 0) -> -1322416027
         * (node2, 0) -> -1074218907
         * (node1, 0) ->  -353732108
         * (node2, 1) ->  -102852506
         * (key1, 0)  ->   133378825
         * (node3, 1) ->   350282656
         * (node1, 1) ->   512294370
         * (node4, 2) ->  1350005242 
         * (node4, 1) ->  1559183344
         * (node3, 2) ->  1809479839
         * (node2, 2) ->  1968027681
         * (key2, 0)  ->  2072764077
         * (node1, 2) ->  2094418500
         */
        clock.tick(10_000);
        group.join("node4");
        while (mapper.size() == 9) {}
        Assert.assertEquals("node3", mapper.map("key1"));
        Assert.assertEquals("node1", mapper.map("key2"));
        
        /**
         * (node5, 2) -> -2014947184
         * (node3, 0) -> -1796370422
         * (node4, 0) -> -1322416027
         * (node2, 0) -> -1074218907
         * (node5, 0) ->  -454886638
         * (node1, 0) ->  -353732108
         * (node5, 1) ->  -187406474
         * (node2, 1) ->  -102852506
         * (key1, 0)  ->   133378825
         * (node3, 1) ->   350282656
         * (node1, 1) ->   512294370
         * (node4, 2) ->  1350005242 
         * (node4, 1) ->  1559183344
         * (node3, 2) ->  1809479839
         * (node2, 2) ->  1968027681
         * (key2, 0)  ->  2072764077
         * (node1, 2) ->  2094418500
         */
        clock.tick(10_000);
        group.join("node5");
        while (mapper.size() == 12) {}
        Assert.assertEquals("node3", mapper.map("key1"));
        Assert.assertEquals("node1", mapper.map("key2"));
        
        /**
         * (node5, 2) -> -2014947184
         * (node3, 0) -> -1796370422
         * (node4, 0) -> -1322416027
         * (node6, 1) -> -1288170696
         * (node2, 0) -> -1074218907
         * (node5, 0) ->  -454886638
         * (node1, 0) ->  -353732108
         * (node5, 1) ->  -187406474
         * (node2, 1) ->  -102852506
         * (key1, 0)  ->   133378825
         * (node6, 2) ->   231893348
         * (node3, 1) ->   350282656
         * (node1, 1) ->   512294370
         * (node6, 0) ->  1151568728
         * (node4, 2) ->  1350005242 
         * (node4, 1) ->  1559183344
         * (node3, 2) ->  1809479839
         * (node2, 2) ->  1968027681
         * (key2, 0)  ->  2072764077
         * (node1, 2) ->  2094418500
         */
        clock.tick(10_000);
        group.join("node6");
        while (mapper.size() == 15) {}
        Assert.assertEquals("node6", mapper.map("key1"));
        Assert.assertEquals("node1", mapper.map("key2"));
        
        /**
         * (node5, 2) -> -2014947184
         * (node3, 0) -> -1796370422
         * (node4, 0) -> -1322416027
         * (node6, 1) -> -1288170696
         * (node2, 0) -> -1074218907
         * (node5, 0) ->  -454886638
         * (node5, 1) ->  -187406474
         * (node2, 1) ->  -102852506
         * (key1, 0)  ->   133378825
         * (node6, 2) ->   231893348
         * (node3, 1) ->   350282656
         * (node6, 0) ->  1151568728
         * (node4, 2) ->  1350005242 
         * (node4, 1) ->  1559183344
         * (node3, 2) ->  1809479839
         * (node2, 2) ->  1968027681
         * (key2, 0)  ->  2072764077
         */
        group.leave("node1");
        while (mapper.size() == 18) {}
        Assert.assertEquals("node6", mapper.map("key1"));
        Assert.assertEquals("node5", mapper.map("key2"));
        
        /**
         * (node5, 2) -> -2014947184
         * (node3, 0) -> -1796370422
         * (node4, 0) -> -1322416027
         * (node6, 1) -> -1288170696
         * (node5, 0) ->  -454886638
         * (node5, 1) ->  -187406474
         * (key1, 0)  ->   133378825
         * (node6, 2) ->   231893348
         * (node3, 1) ->   350282656
         * (node6, 0) ->  1151568728
         * (node4, 2) ->  1350005242 
         * (node4, 1) ->  1559183344
         * (node3, 2) ->  1809479839
         * (key2, 0)  ->  2072764077
         */
        clock.tick(20_000); // node2 expired
        while (mapper.size() == 15) {}
        Assert.assertEquals("node6", mapper.map("key1"));
        Assert.assertEquals("node5", mapper.map("key2"));
        
        /**
         * (node5, 2) -> -2014947184
         * (node4, 0) -> -1322416027
         * (node6, 1) -> -1288170696
         * (node5, 0) ->  -454886638
         * (node5, 1) ->  -187406474
         * (key1, 0)  ->   133378825
         * (node6, 2) ->   231893348
         * (node6, 0) ->  1151568728
         * (node4, 2) ->  1350005242 
         * (node4, 1) ->  1559183344
         * (key2, 0)  ->  2072764077
         */
        group.leave("node3");
        while (mapper.size() == 12) {}
        Assert.assertEquals("node6", mapper.map("key1"));
        Assert.assertEquals("node5", mapper.map("key2"));
        
        /**
         * (node5, 2) -> -2014947184
         * (node6, 1) -> -1288170696
         * (node5, 0) ->  -454886638
         * (node5, 1) ->  -187406474
         * (key1, 0)  ->   133378825
         * (node6, 2) ->   231893348
         * (node6, 0) ->  1151568728
         * (key2, 0)  ->  2072764077
         */
        clock.tick(20_000); // node4 expired
        while (mapper.size() == 9) {}
        Assert.assertEquals("node6", mapper.map("key1"));
        Assert.assertEquals("node5", mapper.map("key2"));
        
        /**
         * (node6, 1) -> -1288170696
         * (key1, 0)  ->   133378825
         * (node6, 2) ->   231893348
         * (node6, 0) ->  1151568728
         * (key2, 0)  ->  2072764077
         */
        group.leave("node5");
        while (mapper.size() == 6) {}
        Assert.assertEquals("node6", mapper.map("key1"));
        Assert.assertEquals("node6", mapper.map("key2"));
        
        /**
         * (key1, 0)  ->   133378825
         * (key2, 0)  ->  2072764077
         */
        clock.tick(20_000); // node6 expired
        while (mapper.size() == 3) {}
        Assert.assertNull(mapper.map("key1"));
        Assert.assertNull(mapper.map("key2"));
      }
      finally
      {
        mapper.shutdown();
      }
    }
    finally
    {
      mgr.deleteGroup("myTestGroup");
    }
  }

}
