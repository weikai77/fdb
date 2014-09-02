package com.weikai77.fdb.util;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

import com.foundationdb.Database;
import com.foundationdb.FDBException;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple;

/**
 * 
 * @author kwei
 *
 */
public class FdbUtils
{
  public static byte[] encode(String s)
  {
    try
    {
      return s.getBytes("UTF8");
    }
    catch (UnsupportedEncodingException ex)
    {
      // this should never happen
      throw new RuntimeException(ex);
    }
  }
  
  public static String decodeString(byte[] bytes)
  {
    try
    {
      return new String(bytes, "UTF8");
    }
    catch (UnsupportedEncodingException ex)
    {
      // this should never happen
      throw new RuntimeException(ex);
    }
  }
  
  public static byte[] encode(long l)
  {
    return ByteArrayUtil.encodeInt(l);
  }
  
  public static long decodeLong(byte[] bytes)
  {
    return ByteArrayUtil.decodeInt(bytes);
  }
  
  public static byte[] get(Database fdb, byte[] key)
  {
    return fdb.run(new Function<Transaction, byte[]>()
    {
      public byte[] apply(Transaction tr)
      {
        Future<byte[]> f = tr.snapshot().get(key);
        return f.get();
      }
    });
  }

  public static void set(Database fdb, byte[] key, byte[] value)
  {
    fdb.run(new Function<Transaction, Void>()
    {
      public Void apply(Transaction tr)
      {
        tr.set(key, value);
        return null;
      }
    });
  }

  public static void clear(Database fdb, byte[] key)
  {
    fdb.run(new Function<Transaction, Void>()
    {
      public Void apply(Transaction tr)
      {
        tr.clear(key);
        return null;
      }
    });
  }

  /**
   * Atomically get the value and set a watch on the key.
   * 
   * @param fdb
   * @param key
   * @return
   */
  public static Watch getAndWatch(Database fdb, ScheduledExecutorService timer, byte[] key)
  {
    return fdb.run(new Function<Transaction, Watch>()
    {
      public Watch apply(Transaction tr)
      {
        return getAndWatch(tr, timer, key);
      }
    });
  }
  
  public static Watch getAndWatch(Transaction tr, ScheduledExecutorService timer, byte[] key)
  {
    Future<byte[]> f = tr.get(key);
    Future<Void> watch = tr.watch(key);
    Watch w = new Watch(key, f.get(), watch, timer);
    return w;
  }
  
  /**
   * Atomically set the value and set a watch on the key.
   * 
   * @param fdb
   * @param key
   * @param value
   * @return
   */
  public static Watch setAndWatch(Database fdb, ScheduledExecutorService timer, byte[] key, byte[] value)
  {
    return fdb.run(new Function<Transaction, Watch>()
    {
      public Watch apply(Transaction tr)
      {
        return setAndWatch(tr, timer, key, value);
      }
    });
  }

  public static Watch setAndWatch(Transaction tr, ScheduledExecutorService timer, byte[] key, byte[] value)
  {
    tr.set(key, value);
    Future<Void> watch = tr.watch(key);
    Watch w = new Watch(key, value, watch, timer);
    return w;
  }

  /**
   * Wake up all watchers waiting on the given key.
   * 
   * @param fdb
   * @param key
   */
  public static void signalWatch(Database fdb, byte[] key)
  {
    fdb.run(new Function<Transaction, Void>()
    {
      public Void apply(Transaction tr)
      {
        signalWatch(tr, key);
        return null;
      }
    });
  }
  
  public static void signalWatch(Transaction tr, byte[] key)
  {
    byte[] value = tr.get(key).get();
    while (true)
    {
      byte[] bytes = new byte[10];
      new Random().nextBytes(bytes);
      if (!Arrays.equals(value, bytes))
      {
        tr.set(key, bytes);
        break;
      }
    }
  }

  /**
   * Atomic compare-and-set.
   * 
   * @param fdb
   * @param key
   * @param expectedValue
   * @param newValue
   * @return
   */
  public static boolean compareAndSet(Database fdb, final byte[] key, final byte[] expectedValue, final byte[] newValue)
  {
    try
    {
      return fdb.run(new Function<Transaction, Boolean>()
      {
        public Boolean apply(Transaction tr)
        {
          if (Arrays.equals(tr.get(key).get(), expectedValue))
          {
            tr.set(key, newValue);
            return true;
          }
          else
          {
            return false;
          }
        }
      });
    }
    catch (FDBException ex)
    {
      return false;
    }
  }
  
  /**
   * Atomic compare-and-clear.
   * 
   * @param fdb
   * @param key
   * @param expectedValue
   * @return
   */
  public static boolean compareAndClear(Database fdb, final byte[] key, final byte[] expectedValue)
  {
    try
    {
      return fdb.run(new Function<Transaction, Boolean>()
      {
        public Boolean apply(Transaction tr)
        {
          if (Arrays.equals(tr.get(key).get(), expectedValue))
          {
            tr.clear(key);
            return true;
          }
          else
          {
            return false;
          }
        }
      });
    }
    catch (FDBException ex)
    {
      return false;
    }
  }

  /**
   * Atomic put-if-absent.
   * 
   * @param fdb
   * @param key
   * @param value
   * @return
   */
  public static boolean putIfAbsent(Database fdb, final byte[] key, final byte[] value)
  {
    return compareAndSet(fdb, key, null, value);
  }

  public static List<String> listChildrenAsStrings(Database fdb, Subspace space)
  {
    return fdb.run(new Function<Transaction,List<String>>()
    {
      @Override
      public List<String> apply(Transaction tr)
      {
        return listChildrenAsStrings(tr, space);
      }
    });
  }

  public static List<String> listChildrenAsStrings(Transaction tr, Subspace space)
  {
    List<String> list = new ArrayList<>();
    Range range = space.range();
    tr.snapshot().getRange(range).forEach(kv -> 
    {
      Tuple tuple = Tuple.fromBytes(kv.getKey());
      String s = tuple.getString(tuple.size()-1); // the last field is the value
      list.add(s);
    });
    
    return list;
  }
}
