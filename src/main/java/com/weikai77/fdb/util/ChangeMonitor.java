package com.weikai77.fdb.util;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.tuple.ByteArrayUtil;
import com.weikai77.fdb.util.concurrent.DistributedSimpleLock;

/**
 * 
 * @author kwei
 *
 */
public class ChangeMonitor
{
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedSimpleLock.class);

  public static interface ChangeListener
  {
    void onChange(byte[] key);
  }

  private final Database _fdb;
  private final ConcurrentHashMap<byte[],Set<ChangeListener>> _listeners;
  private final ConcurrentHashMap<byte[],Future<Void>> _watches;
  
  public ChangeMonitor(Database fdb)
  {
    this._fdb = fdb;
    this._listeners = new ConcurrentHashMap<>();
    this._watches = new ConcurrentHashMap<>();
  }
  
  public void shutdown()
  {
    for (Entry<byte[],Set<ChangeListener>> entry : _listeners.entrySet())
    {
      byte[] key = entry.getKey();
      for (ChangeListener listener : entry.getValue())
      {
        unregisterListener(key, listener);
      }
    }
  }

  /**
   * Clients are expected to retrieve the initial snapshot immediately
   * *after* registering this listener, otherwise they may miss some 
   * changes.
   * 
   * @param key
   * @param listener
   */
  public synchronized void registerListener(byte[] key, ChangeListener listener)
  {
    Set<ChangeListener> listeners = _listeners.get(key);
    if (listeners == null)
    {
      listeners = new HashSet<>();
      _listeners.put(key, listeners);
    }
    
    if (listeners.isEmpty())
    {
      // first listener on this key, so initiate the watch loop
      Future<Void> watch = _fdb.run(new Function<Transaction,Future<Void>>()
      {
        @Override
        public Future<Void> apply(Transaction tr)
        {
          return tr.watch(key);
        }
      });
      
      _watches.put(key, watch);
      watch.onReady(new WatchLoop(key));
    }
    
    listeners.add(listener);
  }
  
  public synchronized boolean unregisterListener(byte[] key, ChangeListener listener)
  {
    Set<ChangeListener> listeners = _listeners.get(key);
    if (listeners == null || listeners.isEmpty())
    {
      return false;
    }
    else
    {
      boolean found = listeners.remove(key);
      if (found && listeners.isEmpty())
      {
        // last listener on this key, so cancel the watch loop
        Future<Void> watch = _watches.remove(key);
        if (watch != null)
        {
          watch.cancel();
        }
      }
      
      return found;
    }
  }

  private class WatchLoop implements Runnable
  {
    private final byte[] _key;

    public WatchLoop(byte[] key)
    {
      this._key = key;
    }

    /**
     *  Invoked when there is a change on the key, or an
     *  error has occurred, on the watch was cancelled.
     */
    @Override
    public void run()
    {
      _fdb.run(new Function<Transaction,Boolean>()
      {
        @Override
        public Boolean apply(Transaction tr)
        {
          // continue watching for the next change
          Future<Void> watch = tr.watch(_key);
          Future<Void> prev = _watches.put(_key, watch);
          if (prev == null)
          {
            // no more listener on this key...break out the loop
            watch.cancel();
            return false;
          }
          else
          {
            prev.cancel();
            watch.onReady(WatchLoop.this);
            return true;
          }
        }
      });
      
      Set<ChangeListener> listeners = _listeners.get(_key);
      if (listeners != null && !listeners.isEmpty())
      {
        // notify the listeners
        for (ChangeListener listener : listeners)
        {
          try
          {
            listener.onChange(_key);
          }
          catch (Exception ex)
          {
            LOGGER.error("Error invoking change listener for key " + 
                ByteArrayUtil.printable(_key), ex);
          }
        }
      }
    }
  }
  
}
