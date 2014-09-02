package com.weikai77.fdb.util;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.foundationdb.Transaction;
import com.foundationdb.async.Future;

/**
 * A wrapper around FDB's watch functionality.
 * 
 * @author kwei
 *
 */
public class Watch
{
  private final byte[] _key;
  private final byte[] _value;
  private final Future<Void> _future;
  private final ScheduledExecutorService _timer;
  private volatile boolean _cancelled = false;
  
  public Watch(byte[] key, byte[] value, Future<Void> future, 
      ScheduledExecutorService timer)
  {
    this._key = key;
    this._value = value;
    this._future = future;
    this._timer = timer;
  }
  
  public byte[] getKey()
  {
    return _key;
  }
  
  public byte[] getValue()
  {
    return _value;
  }

  public void await()
  {
    _future.blockUntilReady();
  }
  
  /**
   * @return false if timed out, true otherwise
   * @throws InterruptedException 
   */
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException
  {
    // set up a canceller to enforce timeout
    ScheduledFuture<?> future = _timer.schedule(new Runnable()
    {
      @Override
      public void run()
      {
        cancel();
      }
    }, timeout, unit);

    await();

    // This actually does not work today because the interrupt seems
    // to be swallowed by the preceding await() call. This will 
    // start to work if/when the FDB folks fix this bug/design flaw.
    if (Thread.interrupted())
    {
      // current thread interrupted--cancel the canceller
      future.cancel(false);
      throw new InterruptedException();
    }
    else if (!_cancelled)
    {
      // received a signal before timeout--cancel the canceller
      future.cancel(false);
      return true;
    }
    else
    {
      // await was cancelled due to timeout
      return false;
    }
  }

  public void signalAll(Transaction tr)
  {
    byte[] value = tr.get(_key).get();
    while (true)
    {
      byte[] bytes = new byte[10];
      new Random().nextBytes(bytes);
      if (!Arrays.equals(value, bytes))
      {
        tr.set(_key, bytes);
        break;
      }
    }
  }
  
  public void cancel()
  {
    _cancelled = true;
    _future.cancel();
  }
  
}
