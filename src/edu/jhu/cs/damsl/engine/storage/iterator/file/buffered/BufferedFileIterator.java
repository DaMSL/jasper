package edu.jhu.cs.damsl.engine.storage.iterator.file.buffered;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.accessor.BufferedPageAccessor;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.iterator.file.BaseStorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.Tuple;

public abstract class BufferedFileIterator<
                          IdType         extends TupleId,
                          HeaderType     extends PageHeader,
                          PageType       extends Page<IdType, HeaderType>,
                          FileType       extends StorageFile<IdType, HeaderType, PageType>>
                        extends BaseStorageFileIterator<IdType, HeaderType, PageType>
{
  public BufferedFileIterator(StorageEngine<IdType, HeaderType, PageType, FileType> e,
                              TransactionId t, Page.Permissions perm, FileType f)
  {
    super(new BufferedPageAccessor<IdType, HeaderType,
                                   PageType, FileType>(e, t, perm, f));
  }

  public BufferedFileIterator(StorageEngine<IdType, HeaderType, PageType, FileType> e,
                              TransactionId t, Page.Permissions perm, FileType f,
                              IdType start, IdType end)
  {
    super(new BufferedPageAccessor<IdType, HeaderType,
                                   PageType, FileType>(e, t, perm, f),
          start, end);
  }

  @Override
  @SuppressWarnings("unchecked")
  public BufferedPageAccessor<IdType, HeaderType, PageType, FileType> getAccessor() { 
    return (BufferedPageAccessor<IdType, HeaderType, PageType, FileType>) super.getAccessor();
  }
}
