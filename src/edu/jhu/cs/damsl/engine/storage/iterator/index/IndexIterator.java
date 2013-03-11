package edu.jhu.cs.damsl.engine.storage.iterator.index;

import java.util.Iterator;

import edu.jhu.cs.damsl.catalog.identifiers.TupleId;

/**
  * A tuple identifer iterator interface for indexes. Implementations of this
  * interface provides a next() method that yields tuple ids (i.e tuple
  * locations in the indexed relation) for all known search key values.
  */
public interface IndexIterator<IdType extends TupleId>
                    extends Iterator<IdType>
{}
