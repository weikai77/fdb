package com.weikai77.fdb.util;

import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;

/**
 * 
 * @author kwei
 *
 */
public class TupleBasedSpace implements Space
{
  private final Subspace _subspace;

  public TupleBasedSpace(Object... path)
  {
    this(new Subspace(Tuple.from(path)));
  }
  
  public TupleBasedSpace(Subspace subspace)
  {
    _subspace = subspace;
  }

  @Override
  public Space subspace(Object... path)
  {
    return new TupleBasedSpace(_subspace.subspace(Tuple.from(path)));
  }

  @Override
  public Subspace rawSubspace(Object... path)
  {
    return new Subspace(getKey()).subspace(Tuple.from(path));
  }

  @Override
  public byte[] getKey()
  {
    return _subspace.getKey();
  }
}
