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

public class IndexFileIterator<IdType extends TupleId>
                implements IndexIterator<IdType>
{
  // Internal iterator used to traverse pages of index files.
  // This is not exposed to the caller, otherwise the caller would directly
  // have access to the tuples stored in index pages, rather than the index entries
  // they represent.
  class InternalIterator extends BaseIndexFileIterator<IdType>
  {
    InternalIterator(DbBufferPool<IndexId, ContiguousTupleId, PageHeader,
                                   IndexPage<IdType>, IndexFile<IdType>> pool,
                     TupleIdFactory<IdType> factory,
                     IndexFile<IdType> file)
    {
      super(pool, factory, file);
    }

    InternalIterator(DbBufferPool<IndexId, ContiguousTupleId, PageHeader,
                                   IndexPage<IdType>, IndexFile<IdType>> pool,
                     TupleIdFactory<IdType> factory,
                     IndexFile<IdType> file,
                     ContiguousTupleId start, ContiguousTupleId end)
    {
      super(pool, factory, file, start, end);
    }

    // Override the nextPageId method in the superclass to use
    // the next page pointer from the leaf index page.
    @Override
    public PageId nextPageId() {
      PageId r = null;
      if ( current == null ) { 
        r = start == null? indexFile.getFirstLeafPage() : start.pageId();
      } 
      else if ( current != null && current.getPage().isLeaf() )
      {
        int nextPageNum = current.getPage().getNextPage().pageNum();
        if ( (end != null && current.getPageId() != end.pageId())
              && nextPageNum < filePages )
        {
          r = new PageId(fileId, nextPageNum);
        }
      }
      return r;
    }
  }
  
  InternalIterator backingIterator;

  public IndexFileIterator(DbBufferPool<IndexId, ContiguousTupleId, PageHeader,
                                        IndexPage<IdType>, IndexFile<IdType>> pool,
                           TupleIdFactory<IdType> factory,
                           IndexFile<IdType> file)
  {
    backingIterator = this.new InternalIterator(pool, factory, file);
  }

  public IndexFileIterator(DbBufferPool<IndexId, ContiguousTupleId, PageHeader,
                                        IndexPage<IdType>, IndexFile<IdType>> pool,
                           TupleIdFactory<IdType> factory,
                           IndexFile<IdType> file,
                           ContiguousTupleId start, ContiguousTupleId end)
  {
    backingIterator = this.new InternalIterator(pool, factory, file, start, end);
  }

  // Index iterator method implementations.
  
  public boolean hasNext() { return backingIterator.hasNext(); }

  // Retrieves the tuple from the underlying iterator, constructs an
  // index entry, and returns that entry's tuple id. This requires the
  // iterator to only traverse leaf pages.
  public IdType next() {
    Tuple t = backingIterator.next();
    IndexEntry<IdType> entry =
      new IndexEntry<IdType>(backingIterator.getFile().getIndexSchema());
    entry.read(t, true, backingIterator.getTupleIdFactory());
    return entry.tuple();
  }

  public void remove() { backingIterator.remove(); }

}