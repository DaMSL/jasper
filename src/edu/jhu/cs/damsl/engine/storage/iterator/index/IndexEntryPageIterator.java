package edu.jhu.cs.damsl.engine.storage.iterator.index;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.IndexId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.index.IndexEntry;
import edu.jhu.cs.damsl.engine.storage.index.IndexFile;
import edu.jhu.cs.damsl.engine.storage.index.IndexPage;
import edu.jhu.cs.damsl.engine.storage.iterator.page.ContiguousPageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;

public class IndexEntryPageIterator<IdType extends TupleId>
                implements IndexEntryIterator<IdType>
{
  IndexPage<IdType> indexPage;
  Schema indexKeySchema;
  TupleIdFactory<IdType> factory;
  ContiguousPageIterator backingIterator;

  public IndexEntryPageIterator(PageId id, IndexPage<IdType> p,
                                Schema key, TupleIdFactory<IdType> f)
  {
    this(id, p, key, f, null, null);
  }

  public IndexEntryPageIterator(PageId id, IndexPage<IdType> p,
                                Schema key, TupleIdFactory<IdType> f,
                                ContiguousTupleId start, ContiguousTupleId end)
  {
    indexPage = p;
    indexKeySchema = key;
    factory = f;
    backingIterator = new ContiguousPageIterator(id, p, start, end);
  }

  public boolean hasNext() { return backingIterator.hasNext(); }

  public IndexEntry<IdType> next() {
    Tuple t = backingIterator.next();
    IndexEntry<IdType> entry = new IndexEntry<IdType>(indexKeySchema);
    entry.read(t, indexPage.isLeaf(), factory);
    return entry;
  }

  public void remove() { backingIterator.remove(); }

}
