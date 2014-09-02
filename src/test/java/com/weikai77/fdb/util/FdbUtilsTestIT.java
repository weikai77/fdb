package com.weikai77.fdb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Assert;
import org.junit.Test;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple;

/**
 * 
 * @author kwei
 *
 */
public class FdbUtilsTestIT
{
  @Test
  public void helloWorld() throws Exception
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();

    {
      // Run an operation on the database
      db.run(new Function<Transaction, Void>() {
        public Void apply(Transaction tr) {
          tr.set(Tuple.from("hello", "world").pack(), Tuple.from("Hello world!").pack());
          return null;
        }
      });
  
      // Get the value of 'hello' from the database
      String hello = db.run(new Function<Transaction, String>() {
        public String apply(Transaction tr) {
          byte[] result = tr.get(Tuple.from("hello", "world").pack()).get();
          return Tuple.fromBytes(result).getString(0);
        }
      });
      System.out.println(hello);
    }

    {
      // Run an operation on the database
      db.run(new Function<Transaction, Void>() {
        public Void apply(Transaction tr) {
          try
          {
            tr.set("hello".getBytes("UTF8"), "world".getBytes());
          }
          catch (Exception ex)
          {
            ex.printStackTrace();
          }
          return null;
        }
      });
  
      // Get the value of 'hello' from the database
      String hello = db.run(new Function<Transaction, String>() {
        public String apply(Transaction tr) {
          try
          {
            byte[] result = tr.get("hello".getBytes()).get();
            return new String(result, "UTF8");
          }
          catch (Exception ex)
          {
            ex.printStackTrace();
            return null;
          }
        }
      });
      System.out.println("Hello " + hello);
    }
  }
  
  @Test
  public void testTuple() throws Exception
  {
    Tuple t1 = Tuple.from("a", "b", "c");
    Tuple t2 = Tuple.from("a", "b", "c");
    Assert.assertEquals("a", t1.get(0));
    Assert.assertEquals("b", t1.get(1));
    Assert.assertEquals("c", t1.get(2));
    Assert.assertEquals("a", t2.get(0));
    Assert.assertEquals("b", t2.get(1));
    Assert.assertEquals("c", t2.get(2));

    Assert.assertEquals(t1, t2);
    Assert.assertEquals(Tuple.fromBytes(t1.pack()), 
                        Tuple.fromBytes(t2.pack()));
    Assert.assertArrayEquals(t1.pack(), t2.pack());
    
    System.out.println(ByteArrayUtil.printable(Tuple.from("hello", "world").pack()));
  }
  
  @Test
  public void testRange() throws Exception
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();

    db.run(new Function<Transaction, Void>()
    {
      public Void apply(Transaction tr)
      {
        try
        {
          tr.set(Tuple.from("key", "a1").pack(), "A1".getBytes("UTF8"));
          tr.set(Tuple.from("key", "a2").pack(), "A2".getBytes("UTF8"));
          tr.set(Tuple.from("key", "a3").pack(), "A3".getBytes("UTF8"));
          List<KeyValue> list = tr.getRange(Tuple.from("key").range()).asList().get();
          Assert.assertEquals(3, list.size());
          Assert.assertEquals(Tuple.from("key", "a1"), Tuple.fromBytes(list.get(0).getKey()));
          Assert.assertEquals("A1", new String(list.get(0).getValue(), "UTF8"));
          Assert.assertEquals(Tuple.from("key", "a2"), Tuple.fromBytes(list.get(1).getKey()));
          Assert.assertEquals("A2", new String(list.get(1).getValue(), "UTF8"));
          Assert.assertEquals(Tuple.from("key", "a3"), Tuple.fromBytes(list.get(2).getKey()));
          Assert.assertEquals("A3", new String(list.get(2).getValue(), "UTF8"));
        }
        catch (Exception ex)
        {
          ex.printStackTrace();
        }
        return null;
      }
    });
  }
  
  @Test
  public void testGetAndWatch() throws Exception
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();

    final Watch watch = FdbUtils.getAndWatch(db, Executors.newSingleThreadScheduledExecutor(),
        "myFirstWatch".getBytes());

    db.run(new Function<Transaction,Void>()
    {
      @Override
      public Void apply(Transaction tr)
      {
        watch.signalAll(tr);
        return null;
      }
    });
    
    watch.await();
  }

  @Test
  public void testSetAndWatch() throws Exception
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();

    final Watch watch = FdbUtils.setAndWatch(db, Executors.newSingleThreadScheduledExecutor(),
        "myFirstWatch".getBytes(), "".getBytes());

    db.run(new Function<Transaction,Void>()
    {
      @Override
      public Void apply(Transaction tr)
      {
        watch.signalAll(tr);
        return null;
      }
    });
    
    watch.await();
  }
  
  @Test
  public void testWatchLimit() throws Exception
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();
    db.options().setMaxWatches(100);
    ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();

    List<Watch> watches = new ArrayList<Watch>();
    for (int i=0; i<200; i++)
    {
      watches.add(FdbUtils.getAndWatch(db, timer, ("watch" + i).getBytes()));
    }
  }

}
