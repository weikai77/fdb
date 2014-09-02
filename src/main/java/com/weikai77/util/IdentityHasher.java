package com.weikai77.util;

/**
 * Mostly for testing.
 * 
 * @author kwei
 *
 */
public class IdentityHasher implements Hasher<Integer>
{
  private static final IdentityHasher _instance = new IdentityHasher();
  
  public static IdentityHasher getInstance()
  {
    return _instance;
  }
  
  private IdentityHasher() {}
  
  @Override
  public int hash(Integer key, int seed)
  {
    return key;
  }
}
