package edu.jhu.cs.damsl.engine.storage.iterator.index;

import java.util.Iterator;

import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.storage.index.IndexEntry;

/**
  * A page iterator interface for index files. An index page is made up
  * of a sequence of index entries, thus this iterator provides a
  * next() method to retrieve entries.
  */
public interface IndexEntryIterator<IdType extends TupleId>
                    extends Iterator<IndexEntry<IdType>>
{}
