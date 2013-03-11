package edu.jhu.cs.damsl.engine.storage.file;

import java.io.FileNotFoundException;
import java.io.IOException;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.FileId.FileKind;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.iterator.file.StorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.buffered.BufferedContiguousFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.ContiguousFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.heap.ContiguousHeapFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.transactions.TransactionAbortException;
import edu.jhu.cs.damsl.factory.page.HeaderFactory;

public class ContiguousHeapFile extends
                HeapFile<ContiguousTupleId, PageHeader, ContiguousPage>
{
  StorageEngine<ContiguousTupleId, PageHeader, ContiguousPage, ContiguousHeapFile> engine;

  public ContiguousHeapFile(StorageEngine<ContiguousTupleId, PageHeader,
                                          ContiguousPage, ContiguousHeapFile> e,
                            FileKind k, Integer pageSize, Long capacity)
      throws FileNotFoundException
  {
    this(e, k, pageSize, capacity, null);
  }

  public ContiguousHeapFile(StorageEngine<ContiguousTupleId, PageHeader,
                                          ContiguousPage, ContiguousHeapFile> e,
                            FileKind k, Integer pageSz, Long capacity, Schema sch)
      throws FileNotFoundException
  {
    super(k, pageSz, capacity, sch);
    engine = e;
  }

  public ContiguousHeapFile(StorageEngine<ContiguousTupleId, PageHeader,
                                          ContiguousPage, ContiguousHeapFile> e,
                            FileId id, Schema sch, TableId rel)
      throws FileNotFoundException
  {
    super(id, sch, rel);
    engine = e;
  }

  protected void validateTupleRange(ContiguousTupleId start, ContiguousTupleId end) 
    throws TransactionAbortException
  {
    boolean valid =
      start.pageId().pageNum() < end.pageId().pageNum()
        || (start.pageId().pageNum() == end.pageId().pageNum()
            && start.offset() < end.offset());

    if ( !valid ) { throw new TransactionAbortException(); }
  }

  public StorageIterator iterator() {
    return new ContiguousHeapFileIterator(engine.getBufferPool(), this);
  }

  public StorageIterator iterator(ContiguousTupleId start, ContiguousTupleId end)
    throws TransactionAbortException
  {
    validateTupleRange(start, end);
    return new ContiguousHeapFileIterator(engine.getBufferPool(), this, start, end);
  }

  public StorageFileIterator<ContiguousTupleId, PageHeader, ContiguousPage>
  heap_iterator() { 
    return new ContiguousHeapFileIterator(engine.getBufferPool(), this);
  }

  public StorageFileIterator<ContiguousTupleId, PageHeader, ContiguousPage>
  heap_iterator(ContiguousTupleId start, ContiguousTupleId end)
    throws TransactionAbortException
  { 
    validateTupleRange(start, end);
    return new ContiguousHeapFileIterator(engine.getBufferPool(), this, start, end);
  }

  public StorageFileIterator<ContiguousTupleId, PageHeader, ContiguousPage>
  buffered_iterator(TransactionId txn, Page.Permissions perm) { 
    return new BufferedContiguousFileIterator(engine, txn, perm, this);
  }

  public StorageFileIterator<ContiguousTupleId, PageHeader, ContiguousPage>
  buffered_iterator(TransactionId txn, Page.Permissions perm,
                    ContiguousTupleId start, ContiguousTupleId end)
    throws TransactionAbortException
  { 
    validateTupleRange(start, end);
    return new BufferedContiguousFileIterator(engine, txn, perm, this, start, end);
  }

  // Returns a direct iterator over on-disk page headers.
  public ContiguousFileHeaderIterator header_iterator() {
    return new ContiguousFileHeaderIterator(this);
  }
  
  public HeaderFactory<PageHeader> getHeaderFactory() {
    return ContiguousPage.headerFactory;
  }
}