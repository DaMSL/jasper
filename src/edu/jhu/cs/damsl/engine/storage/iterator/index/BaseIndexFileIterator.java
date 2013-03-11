package edu.jhu.cs.damsl.engine.storage.iterator.index;

import edu.jhu.cs.damsl.catalog.identifiers.IndexId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.iterator.file.BaseStorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.accessor.HeapFileAccessor;
import edu.jhu.cs.damsl.engine.storage.index.IndexEntry;
import edu.jhu.cs.damsl.engine.storage.index.IndexFile;
import edu.jhu.cs.damsl.engine.storage.index.IndexPage;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;

public class BaseIndexFileIterator<IdType extends TupleId>
               extends BaseStorageFileIterator<ContiguousTupleId, PageHeader, IndexPage<IdType>>
{
  IndexFile<IdType> indexFile;
  TupleIdFactory<IdType> tupleIdFactory;

  public BaseIndexFileIterator(DbBufferPool<IndexId, ContiguousTupleId, PageHeader,
                                            IndexPage<IdType>, IndexFile<IdType>> pool,
                               TupleIdFactory<IdType> factory,
                               IndexFile<IdType> file)
  {
    super(new HeapFileAccessor<IndexId, ContiguousTupleId, PageHeader, 
                               IndexPage<IdType>, IndexFile<IdType>>(pool, file));
    indexFile = file;
    tupleIdFactory = factory;
  }

  public BaseIndexFileIterator(DbBufferPool<IndexId, ContiguousTupleId, PageHeader,
                                            IndexPage<IdType>, IndexFile<IdType>> pool,
                               TupleIdFactory<IdType> factory,
                               IndexFile<IdType> file,
                               ContiguousTupleId start, ContiguousTupleId end)
  {
    super(new HeapFileAccessor<IndexId, ContiguousTupleId, PageHeader, 
                               IndexPage<IdType>, IndexFile<IdType>>(pool, file),
          start, end);
    indexFile = file;
    tupleIdFactory = factory;
  }

  public IndexFile<IdType> getFile() { return indexFile; }

  public TupleIdFactory<IdType> getTupleIdFactory() { return tupleIdFactory; }

  @Override
  @SuppressWarnings("unchecked")
  public HeapFileAccessor<IndexId, ContiguousTupleId,
                          PageHeader, IndexPage<IdType>, IndexFile<IdType>>
  getAccessor() {
    return (HeapFileAccessor<IndexId, ContiguousTupleId, PageHeader, 
                             IndexPage<IdType>, IndexFile<IdType>>)
            super.getAccessor();
  }

}
