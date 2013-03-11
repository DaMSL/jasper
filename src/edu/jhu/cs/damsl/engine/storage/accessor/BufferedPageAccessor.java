package edu.jhu.cs.damsl.engine.storage.accessor;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public class BufferedPageAccessor<
                IdType extends TupleId,
                HeaderType extends PageHeader,
                PageType extends Page<IdType, HeaderType>,
                FileType extends StorageFile<IdType, HeaderType, PageType>>
              implements PageFileAccessor<IdType, HeaderType, PageType>
{
  StorageEngine<IdType, HeaderType, PageType, FileType> storage;
  TransactionId txn;
  Page.Permissions permissions;
  FileType file;

  public BufferedPageAccessor(StorageEngine<IdType, HeaderType, PageType, FileType> e,
                              TransactionId t, Page.Permissions perm, FileType f)
  {
    storage = e;
    txn = t;
    permissions = perm;
    file = f;
  }

  public TransactionId getTransactionId() { return txn; }
  
  public FileType getFile() { return file; }
  
  public Integer getPageSize() { return file.pageSize(); }

  public FileId getFileId() { return file.fileId(); }

  public int getNumPages() { return file.numPages(); }

  public PageType getPage() throws InterruptedException {
    return storage.getBufferPool().getPage();
  }

  public PageType getPage(PageId pId) {
    return storage.getPage(txn, pId, permissions);
  }

  public void releasePage(PageType p) {
    // A no-op since we hold on to pages until transaction end.
    //storage.releasePage(txn, p);
  }
  
}
