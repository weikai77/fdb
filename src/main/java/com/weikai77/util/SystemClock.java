package com.weikai77.util;

/**
 * 
 * @author kwei
 *
 */
public class SystemClock implements Clock
{
  private static final SystemClock _instance = new SystemClock();
  
  public static SystemClock getInstance()
  {
    return _instance;
  }
  
  private SystemClock() {}

  @Override
  public long currentTimeMillis()
  {
    return System.currentTimeMillis();
  }

}
