package com.weikai77.util;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author kwei
 *
 */
public class ConsistentKeyMapperTest
{
  @Test
  public void testIdentityHasher1() throws Exception
  {
    testIdentityHasher(1);
  }
  
  @Test
  public void testIdentityHasher3() throws Exception
  {
    testIdentityHasher(3);
  }
  
  private void testIdentityHasher(int replicationFactor)
  {
    ConsistentKeyMapper<Integer,Integer> mapper = new ConsistentKeyMapper<>(Collections.emptyList(), 
        IdentityHasher.getInstance(), IdentityHasher.getInstance(), replicationFactor);
    
    Assert.assertNull(mapper.map(1));
    Assert.assertNull(mapper.map(7));
    Assert.assertNull(mapper.map(14));
    
    mapper.add(10);
    Assert.assertEquals(10, mapper.map(1).intValue());
    Assert.assertEquals(10, mapper.map(7).intValue());
    Assert.assertEquals(10, mapper.map(14).intValue());

    mapper.add(5);
    Assert.assertEquals(5, mapper.map(1).intValue());
    Assert.assertEquals(10, mapper.map(7).intValue());
    Assert.assertEquals(5, mapper.map(14).intValue());

    mapper.add(20);
    Assert.assertEquals(5, mapper.map(1).intValue());
    Assert.assertEquals(10, mapper.map(7).intValue());
    Assert.assertEquals(20, mapper.map(14).intValue());

    mapper.add(1);
    Assert.assertEquals(1, mapper.map(1).intValue());
    Assert.assertEquals(10, mapper.map(7).intValue());
    Assert.assertEquals(20, mapper.map(14).intValue());

    mapper.add(7);
    Assert.assertEquals(1, mapper.map(1).intValue());
    Assert.assertEquals(7, mapper.map(7).intValue());
    Assert.assertEquals(20, mapper.map(14).intValue());

    mapper.add(14);
    Assert.assertEquals(1, mapper.map(1).intValue());
    Assert.assertEquals(7, mapper.map(7).intValue());
    Assert.assertEquals(14, mapper.map(14).intValue());
  }
  
  @Test
  public void testXXHasher1()
  {
    ConsistentKeyMapper<String,String> mapper = new ConsistentKeyMapper<>(Collections.emptyList(), 
        XXHasher.getInstance(), XXHasher.getInstance(), 1);

    Assert.assertNull(mapper.map("key1"));
    Assert.assertNull(mapper.map("key2"));
    
    /**
     * (node1, 0) ->  -353732108
     * (key1, 0)  ->   133378825
     * (key2, 0)  ->  2072764077
     */
    mapper.add("node1");
    Assert.assertEquals("node1", mapper.map("key1"));
    Assert.assertEquals("node1", mapper.map("key2"));
    
    /**
     * (node2, 0) -> -1074218907
     * (node1, 0) ->  -353732108
     * (key1, 0)  ->   133378825
     * (key2, 0)  ->  2072764077
     */
    mapper.add("node2");
    Assert.assertEquals("node2", mapper.map("key1"));
    Assert.assertEquals("node2", mapper.map("key2"));
    
    /**
     * (node3, 0) -> -1796370422
     * (node2, 0) -> -1074218907
     * (node1, 0) ->  -353732108
     * (key1, 0)  ->   133378825
     * (key2, 0)  ->  2072764077
     */
    mapper.add("node3");
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
    mapper.add("node4");
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
    mapper.add("node5");
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
    mapper.add("node6");
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
    mapper.remove("node1");
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
    mapper.remove("node2");
    Assert.assertEquals("node6", mapper.map("key1"));
    Assert.assertEquals("node3", mapper.map("key2"));
    
    /**
     * (node4, 0) -> -1322416027
     * (node5, 0) ->  -454886638
     * (key1, 0)  ->   133378825
     * (node6, 0) ->  1151568728
     * (key2, 0)  ->  2072764077
     */
    mapper.remove("node3");
    Assert.assertEquals("node6", mapper.map("key1"));
    Assert.assertEquals("node4", mapper.map("key2"));
    
    /**
     * (node5, 0) ->  -454886638
     * (key1, 0)  ->   133378825
     * (node6, 0) ->  1151568728
     * (key2, 0)  ->  2072764077
     */
    mapper.remove("node4");
    Assert.assertEquals("node6", mapper.map("key1"));
    Assert.assertEquals("node5", mapper.map("key2"));
    
    /**
     * (key1, 0)  ->   133378825
     * (node6, 0) ->  1151568728
     * (key2, 0)  ->  2072764077
     */
    mapper.remove("node5");
    Assert.assertEquals("node6", mapper.map("key1"));
    Assert.assertEquals("node6", mapper.map("key2"));

    /**
     * (key1, 0)  ->   133378825
     * (key2, 0)  ->  2072764077
     */
    mapper.remove("node6");
    Assert.assertNull(mapper.map("key1"));
    Assert.assertNull(mapper.map("key2"));
  }

  @Test
  public void testXXHasher3()
  {
    ConsistentKeyMapper<String,String> mapper = new ConsistentKeyMapper<>(Collections.emptyList(), 
        XXHasher.getInstance(), XXHasher.getInstance(), 3);

    Assert.assertNull(mapper.map("key1"));
    Assert.assertNull(mapper.map("key2"));

    /**
     * (node1, 0) ->  -353732108
     * (key1, 0)  ->   133378825
     * (node1, 1) ->   512294370
     * (key2, 0)  ->  2072764077
     * (node1, 2) ->  2094418500
     */
    mapper.add("node1");
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
    mapper.add("node2");
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
    mapper.add("node3");
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
    mapper.add("node4");
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
    mapper.add("node5");
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
    mapper.add("node6");
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
    mapper.remove("node1");
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
    mapper.remove("node2");
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
    mapper.remove("node3");
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
    mapper.remove("node4");
    Assert.assertEquals("node6", mapper.map("key1"));
    Assert.assertEquals("node5", mapper.map("key2"));
    
    /**
     * (node6, 1) -> -1288170696
     * (key1, 0)  ->   133378825
     * (node6, 2) ->   231893348
     * (node6, 0) ->  1151568728
     * (key2, 0)  ->  2072764077
     */
    mapper.remove("node5");
    Assert.assertEquals("node6", mapper.map("key1"));
    Assert.assertEquals("node6", mapper.map("key2"));
    
    /**
     * (key1, 0)  ->   133378825
     * (key2, 0)  ->  2072764077
     */
    mapper.remove("node6");
    Assert.assertNull(mapper.map("key1"));
    Assert.assertNull(mapper.map("key2"));
  }
    
}
