package edu.jhu.cs.damsl.engine.storage.iterator.index;

import edu.jhu.cs.damsl.catalog.identifiers.IndexId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.index.IndexEntry;
import edu.jhu.cs.damsl.engine.storage.index.IndexFile;
import edu.jhu.cs.damsl.engine.storage.index.IndexPage;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;

public class IndexEntryFileIterator<IdType extends TupleId>
                implements IndexEntryIterator<IdType>
{
  // Internal iterator used to traverse pages of index files.
  // This is not exposed to the caller, otherwise the caller would directly
  // have access to the tuples stored in index pages, rather than the index entries
  // they represent.
  // Unlike the index file iterator, iterating over index entries in
  // an index file does not require us to override the nextPageId method.
  // This ensures that we traverse all pages in the index file, and this
  // iterator provides no guarantees on the ordering of leaf or non-leaf
  // pages encountered.
  class InternalIterator extends BaseIndexFileIterator<IdType>
  {
    InternalIterator(DbBufferPool<IndexId, ContiguousTupleId, PageHeader,
                                  IndexPage<IdType>, IndexFile<IdType>> pool,
                     TupleIdFactory<IdType> factory,
                     IndexFile<IdType> file)
    {
      super(pool, factory, file);
    }

    public IndexPage<IdType> getPage() {
      return current == null ? null : current.getPage();
    }
  }
  
  InternalIterator backingIterator;

  public IndexEntryFileIterator(DbBufferPool<IndexId, ContiguousTupleId, PageHeader,
                                             IndexPage<IdType>, IndexFile<IdType>> pool,
                                TupleIdFactory<IdType> factory,
                                IndexFile<IdType> file)
  {
    backingIterator = this.new InternalIterator(pool, factory, file);
  }

  // Index iterator method implementations.
  
  public boolean hasNext() { return backingIterator.hasNext(); }

  // Retrieves the tuple from the underlying iterator, constructs an
  // index entry, and returns that entry's tuple id. This requires the
  // iterator to only traverse leaf pages.
  public IndexEntry<IdType> next() {
    Tuple t = backingIterator.next();
    IndexEntry<IdType> entry =
      new IndexEntry<IdType>(backingIterator.getFile().getIndexSchema());
    boolean leaf = backingIterator.getPage().isLeaf();
    entry.read(t, leaf, backingIterator.getTupleIdFactory());
    return entry;
  }

  public void remove() { backingIterator.remove(); }

}