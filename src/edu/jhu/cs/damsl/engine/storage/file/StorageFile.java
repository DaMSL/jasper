package edu.jhu.cs.damsl.engine.storage.file;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.engine.storage.iterator.file.StorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.StorageFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.transactions.TransactionAbortException;
import edu.jhu.cs.damsl.factory.page.HeaderFactory;

public interface StorageFile<IdType     extends TupleId,
                             HeaderType extends PageHeader, 
                             PageType   extends Page<IdType, HeaderType>>
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

  public void extend(int pageCount);
  public void shrink(int pageCount);
  
  public void initializePage(PageType buf);
  public int readPage(PageType buf, PageId id);
  public int writePage(PageType p);

  public HeaderFactory<HeaderType> getHeaderFactory();
  public HeaderType readPageHeader(PageId id);

  // Iterator variants.
  public StorageIterator iterator();
  
  public StorageIterator iterator(IdType start, IdType end)
    throws TransactionAbortException;
  
  public StorageFileIterator<IdType, HeaderType, PageType>
  heap_iterator();

  public StorageFileIterator<IdType, HeaderType, PageType>
  heap_iterator(IdType start, IdType end)
    throws TransactionAbortException;
  
  public StorageFileIterator<IdType, HeaderType, PageType>
  buffered_iterator(TransactionId txn, Page.Permissions perm);

  public StorageFileIterator<IdType, HeaderType, PageType>
  buffered_iterator(TransactionId txn, Page.Permissions perm, IdType start, IdType end)
    throws TransactionAbortException;

  public StorageFileHeaderIterator<IdType, HeaderType, PageType> header_iterator();

}
