package com.weikai77.fdb.util;

import java.util.ArrayList;
import java.util.List;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;

/**
 * 
 * @author kwei
 *
 */
public class DirectoryBasedSpace implements Space
{
  private final Database _fdb;
  private final DirectorySubspace _directory;
  
  public DirectoryBasedSpace(Database fdb, Object... path)
  {
    this(fdb, DirectoryUtils.createOrOpen(fdb, path));
  }

  private DirectoryBasedSpace(Database fdb, DirectorySubspace directory)
  {
    _fdb = fdb;
    _directory = directory;
  }
  
  public DirectorySubspace getDirectory()
  {
    return _directory;
  }
  
  public List<String> listChildren()
  {
    return _fdb.run(new Function<Transaction,List<String>>()
    {
      @Override
      public List<String> apply(Transaction tr)
      {
        return _directory.list(tr).get();
      }
    });
  }
  
  public void delete(Object... path)
  {
    DirectoryUtils.delete(_fdb, _directory, path);
  }

  @Override
  public DirectoryBasedSpace subspace(Object... path)
  {
    return new DirectoryBasedSpace(_fdb, _directory.createOrOpen(_fdb, toStringList(path)).get());
  }

  @Override
  public Subspace rawSubspace(Object... path)
  {
    return new Subspace(getKey()).subspace(Tuple.from(path));
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

  @Override
  public byte[] getKey()
  {
    return _directory.getKey();
  }

}
