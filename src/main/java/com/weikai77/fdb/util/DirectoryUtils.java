package com.weikai77.fdb.util;

import java.util.ArrayList;
import java.util.List;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;

/**
 * 
 * @author kwei
 *
 */
public class DirectoryUtils
{
  public static DirectorySubspace createOrOpen(Database fdb, Object... path)
  {
    DirectoryLayer layer = DirectoryLayer.getDefault();
    return fdb.run(new Function<Transaction,DirectorySubspace>()
    {
      @Override
      public DirectorySubspace apply(Transaction tr)
      {
        return layer.createOrOpen(tr, toStringList(path)).get();
      }
    });
  }

  public static DirectorySubspace createOrOpen(Database fdb, DirectorySubspace directory, Object... subpath)
  {
    return fdb.run(new Function<Transaction,DirectorySubspace>()
    {
      @Override
      public DirectorySubspace apply(Transaction tr)
      {
        return directory.createOrOpen(tr, toStringList(subpath)).get();
      }
    });
  }

  public static boolean delete(Database fdb, DirectorySubspace directory, Object... subpath)
  {
    return fdb.run(new Function<Transaction,Boolean>()
    {
      @Override
      public Boolean apply(Transaction tr)
      {
        return directory.removeIfExists(tr, toStringList(subpath)).get();
      }
    });
  }

  private static List<String> toStringList(Object... objects)
  {
    List<String> list = new ArrayList<>();
    for (Object obj : objects)
    {
      list.add(obj.toString());
    }
    return list;
  }


}
