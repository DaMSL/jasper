package edu.jhu.cs.damsl.engine.storage.iterator.file.buffered;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.accessor.BufferedPageAccessor;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.iterator.file.BaseStorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.Tuple;

public abstract class BufferedFileIterator<
                          HeaderType     extends PageHeader,
                          PageType       extends Page<HeaderType>,
                          FileType       extends StorageFile<HeaderType, PageType>>
                        extends BaseStorageFileIterator<HeaderType, PageType>
{
  public BufferedFileIterator(StorageEngine<HeaderType, PageType, FileType> e,
                              TransactionId t, Page.Permissions perm, FileType f)
  {
    super(new BufferedPageAccessor<HeaderType, PageType, FileType>(e, t, perm, f));
  }

  @Override
  @SuppressWarnings("unchecked")
  public BufferedPageAccessor<HeaderType, PageType, FileType> getAccessor() { 
    return (BufferedPageAccessor<HeaderType, PageType, FileType>) super.getAccessor();
  }
}
