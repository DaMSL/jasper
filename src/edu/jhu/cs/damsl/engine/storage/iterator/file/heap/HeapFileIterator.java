package edu.jhu.cs.damsl.engine.storage.iterator.file.heap;

import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.accessor.HeapFileAccessor;
import edu.jhu.cs.damsl.engine.storage.file.HeapFile;
import edu.jhu.cs.damsl.engine.storage.iterator.file.BaseStorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public abstract class HeapFileIterator<
                          HeaderType extends PageHeader,
                          PageType extends Page<HeaderType>,
                          FileType extends HeapFile<HeaderType, PageType>>
                        extends BaseStorageFileIterator<HeaderType, PageType>
{

  public HeapFileIterator(DbBufferPool<HeaderType, PageType, FileType> pool, FileType f)
  {
    super(new HeapFileAccessor<HeaderType, PageType, FileType>(pool, f));
  }

  @Override
  @SuppressWarnings("unchecked")
  public HeapFileAccessor<HeaderType, PageType, FileType> getAccessor() {
    return (HeapFileAccessor<HeaderType, PageType, FileType>) super.getAccessor();
  }

}
