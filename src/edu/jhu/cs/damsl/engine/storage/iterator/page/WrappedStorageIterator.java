package edu.jhu.cs.damsl.engine.storage.iterator.page;

import java.util.Iterator;

import edu.jhu.cs.damsl.engine.storage.Tuple;

public class WrappedStorageIterator implements StorageIterator {

  Iterator<Tuple> wrapped;
  public WrappedStorageIterator(Iterator<Tuple> it) { wrapped = it; }
  
  @Override
  public boolean hasNext() { return wrapped.hasNext(); }

  @Override
  public Tuple next() { return wrapped.next(); }

  @Override
  public void remove() { wrapped.remove(); }

}
