package com.weikai77.fdb.util;

import org.junit.Assert;
import org.junit.Test;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.directory.DirectorySubspace;

/**
 * 
 * @author kwei
 *
 */
public class DirectoryUtilsTestIT
{
  @Test
  public void testDeleteDir() throws Exception
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();

    // create a dir
    DirectorySubspace dir = DirectoryUtils.createOrOpen(db, "test", "myDir");
    byte[] key = dir.pack("myKey");
    byte[] val = "myVal".getBytes();
    try
    {
      // set a key under the dir
      FdbUtils.set(db, key, val);
      Assert.assertArrayEquals(val, FdbUtils.get(db, key));

      // make sure the key is gone after the dir is removed
      dir.remove(db).get();
      Assert.assertFalse(dir.exists(db).get());
      Assert.assertNull(FdbUtils.get(db, key));

      // make sure the key does not resurface after the dir is recreated
      dir = DirectoryUtils.createOrOpen(db, "test", "myDir");
      Assert.assertTrue(dir.exists(db).get());
      Assert.assertNull(FdbUtils.get(db, key));
    }
    finally
    {
      dir.removeIfExists(db).get();
      Assert.assertFalse(dir.exists(db).get());
    }
  }

  @Test
  public void testDeleteSubDir() throws Exception
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();

    // create a dir and a sub dir
    DirectorySubspace dir = DirectoryUtils.createOrOpen(db, "test", "myDir");
    DirectorySubspace subdir = DirectoryUtils.createOrOpen(db, dir, "mySubDir");
    byte[] key = subdir.pack("myKey");
    byte[] val = "myVal".getBytes();
    try
    {
      // set a key under the sub dir
      FdbUtils.set(db, key, val);
      Assert.assertArrayEquals(val, FdbUtils.get(db, key));

      // make sure the key is gone after the sub dir is removed
      subdir.remove(db).get();
      Assert.assertFalse(subdir.exists(db).get());
      Assert.assertNull(FdbUtils.get(db, key));

      // make sure the key does not resurface after the sub dir is recreated
      subdir = DirectoryUtils.createOrOpen(db, dir, "mySubDir");
      Assert.assertTrue(subdir.exists(db).get());
      Assert.assertNull(FdbUtils.get(db, key));
    }
    finally
    {
      dir.removeIfExists(db).get();
      Assert.assertFalse(dir.exists(db).get());
    }
  }

  @Test
  public void testDeleteParentDir() throws Exception
  {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();

    // create a dir and a sub dir
    DirectorySubspace dir = DirectoryUtils.createOrOpen(db, "test", "myDir");
    DirectorySubspace subdir = DirectoryUtils.createOrOpen(db, dir, "mySubDir");
    byte[] key = subdir.pack("myKey");
    byte[] val = "myVal".getBytes();
    try
    {
      // set a key under the sub dir
      FdbUtils.set(db, key, val);
      Assert.assertArrayEquals(val, FdbUtils.get(db, key));

      // make sure the key is gone after the parent dir is removed
      dir.remove(db).get();
      Assert.assertFalse(dir.exists(db).get());
      Assert.assertFalse(subdir.exists(db).get());
      Assert.assertNull(FdbUtils.get(db, key));

      // make sure the key does not resurface after the parent dir is recreated
      dir = DirectoryUtils.createOrOpen(db, "myDir");
      Assert.assertTrue(dir.exists(db).get());
      Assert.assertFalse(subdir.exists(db).get());
      Assert.assertNull(FdbUtils.get(db, key));

      // make sure the key does not resurface after the parent dir is recreated
      subdir = DirectoryUtils.createOrOpen(db, dir, "mySubDir");
      Assert.assertTrue(dir.exists(db).get());
      Assert.assertTrue(subdir.exists(db).get());
      Assert.assertNull(FdbUtils.get(db, key));
    }
    finally
    {
      dir.removeIfExists(db).get();
      Assert.assertFalse(dir.exists(db).get());
    }
  }
  
}
