package edu.jhu.cs.damsl.engine.storage.iterator.file.heap;

import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.accessor.HeapFileAccessor;
import edu.jhu.cs.damsl.engine.storage.file.HeapFile;
import edu.jhu.cs.damsl.engine.storage.iterator.file.BaseStorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public abstract class HeapFileIterator<
                          EntityIdType,
                          IdType      extends TupleId,
                          HeaderType  extends PageHeader,
                          PageType    extends Page<IdType, HeaderType>,
                          FileType    extends HeapFile<IdType, HeaderType, PageType>>
                        extends BaseStorageFileIterator<IdType, HeaderType, PageType>
{

  public HeapFileIterator(DbBufferPool<EntityIdType, IdType,
                                       HeaderType, PageType, FileType> pool,
                          FileType f)
  {
    super(new HeapFileAccessor<EntityIdType, IdType, HeaderType, PageType, FileType>(pool, f));
  }

  public HeapFileIterator(DbBufferPool<EntityIdType, IdType,
                                       HeaderType, PageType, FileType> pool,
                          FileType f, IdType start, IdType end)
  {
    super(new HeapFileAccessor<EntityIdType, IdType, HeaderType, PageType, FileType>(pool, f),
          start, end);
  }

  @Override
  @SuppressWarnings("unchecked")
  public HeapFileAccessor<EntityIdType, IdType, HeaderType, PageType, FileType> getAccessor() {
    return (HeapFileAccessor<EntityIdType, IdType, HeaderType, PageType, FileType>) super.getAccessor();
  }

}
