package edu.jhu.cs.damsl.engine.storage.file;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.engine.storage.iterator.file.StorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.StorageFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;

public interface StorageFile<HeaderType extends PageHeader, 
                             PageType extends Page<HeaderType>>
{
  public FileId fileId();

  public boolean isSorted();

  // Returns null for files that store multiple relations.
  public Schema getSchema();

  // Returns null for temporary files, or files that store multiple relations.
  public TableId getRelation();
  public void setRelation(TableId rel);

  public int pageSize();
  public int numPages();  

  public long size();  
  public long capacity();
  public long remaining();

  public StorageIterator iterator();
  public StorageFileHeaderIterator<HeaderType> header_iterator();
  public StorageFileIterator<HeaderType, PageType> heap_iterator();
  
  public StorageFileIterator<HeaderType, PageType>
  buffered_iterator(TransactionId txn, Page.Permissions perm);

  public void extend(int pageCount);
  public void shrink(int pageCount);
  
  public void initializePage(PageType buf);
  public int readPage(PageType buf, PageId id);
  public int writePage(PageType p);

  public HeaderFactory<HeaderType> getHeaderFactory();
  public HeaderType readPageHeader(PageId id);

}
