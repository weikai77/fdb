package com.weikai77.util;

/**
 * 
 * @author kwei
 *
 */
public class SettableClock implements Clock
{
  private volatile long _time;
  
  public SettableClock(long time)
  {
    _time = time;
  }

  @Override
  public long currentTimeMillis()
  {
    return _time;
  }
  
  public void tick(long millis)
  {
    _time += millis;
  }

}
