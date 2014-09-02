package com.weikai77.util;

import java.io.UnsupportedEncodingException;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * 
 * @author kwei
 *
 */
public class XXHasher implements Hasher<String>
{
  
  private static final XXHasher _instance = new XXHasher();
  
  public static XXHasher getInstance()
  {
    return _instance;
  }
  
  private XXHasher() {}

  @Override
  public int hash(String key, int seed)
  {
    XXHashFactory factory = XXHashFactory.fastestInstance();
    byte[] data = null;
    try
    {
      data = key.getBytes("UTF-8");
    }
    catch (UnsupportedEncodingException ex)
    {
      throw new RuntimeException(ex);
    }

    XXHash32 hash32 = factory.hash32();
    return hash32.hash(data, 0, data.length, seed);
  }

}
