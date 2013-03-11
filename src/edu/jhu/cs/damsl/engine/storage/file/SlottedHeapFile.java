package edu.jhu.cs.damsl.engine.storage.file;

import java.io.FileNotFoundException;
import java.io.IOException;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.FileId.FileKind;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.SlottedTupleId;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.iterator.file.StorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.buffered.BufferedSlottedFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.SlottedFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.heap.SlottedHeapFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.transactions.TransactionAbortException;
import edu.jhu.cs.damsl.factory.page.HeaderFactory;

public class SlottedHeapFile extends 
                HeapFile<SlottedTupleId, SlottedPageHeader, SlottedPage>
{
  StorageEngine<SlottedTupleId, SlottedPageHeader, SlottedPage, SlottedHeapFile> engine;

  public SlottedHeapFile(StorageEngine<SlottedTupleId, SlottedPageHeader,
                                       SlottedPage, SlottedHeapFile> e,
                         FileKind k, Integer pageSize, Long capacity)
      throws FileNotFoundException
  {
    this(e, k, pageSize, capacity, null);
  }

  public SlottedHeapFile(StorageEngine<SlottedTupleId, SlottedPageHeader,
                                       SlottedPage, SlottedHeapFile> e,
                         FileKind k, Integer pageSz, Long capacity, Schema sch)
      throws FileNotFoundException
  {
    super(k, pageSz, capacity, sch);
    engine = e;
  }

  public SlottedHeapFile(StorageEngine<SlottedTupleId, SlottedPageHeader,
                                       SlottedPage, SlottedHeapFile> e,
                         FileId id, Schema sch, TableId rel)
      throws FileNotFoundException
  {
    super(id, sch, rel);
    engine = e;
  }
  
  protected void validateTupleRange(SlottedTupleId start, SlottedTupleId end) 
    throws TransactionAbortException
  {
    boolean valid =
      start.pageId().pageNum() < end.pageId().pageNum()
        || (start.pageId().pageNum() == end.pageId().pageNum()
            && start.slot() < end.slot());

    if ( !valid ) { throw new TransactionAbortException(); }
  }

  // Yields a direct iterator over the file's on-disk pages, using the 
  // given allocator to acquire in-memory page buffers into which to read.
  public StorageIterator iterator() { 
    return new SlottedHeapFileIterator(engine.getBufferPool(), this);
  }

  public StorageIterator iterator(SlottedTupleId start, SlottedTupleId end)
    throws TransactionAbortException
  { 
    validateTupleRange(start, end);
    return new SlottedHeapFileIterator(engine.getBufferPool(), this, start, end);
  }

  public StorageFileIterator<SlottedTupleId, SlottedPageHeader, SlottedPage>
  heap_iterator()
  { 
    return new SlottedHeapFileIterator(engine.getBufferPool(), this);
  }

  public StorageFileIterator<SlottedTupleId, SlottedPageHeader, SlottedPage>
  heap_iterator(SlottedTupleId start, SlottedTupleId end)
    throws TransactionAbortException
  { 
    validateTupleRange(start, end);
    return new SlottedHeapFileIterator(engine.getBufferPool(), this, start, end);
  }

  public StorageFileIterator<SlottedTupleId, SlottedPageHeader, SlottedPage>
  buffered_iterator(TransactionId txn, Page.Permissions perm)
  { 
    return new BufferedSlottedFileIterator(engine, txn, perm, this);
  }

  public StorageFileIterator<SlottedTupleId, SlottedPageHeader, SlottedPage>
  buffered_iterator(TransactionId txn, Page.Permissions perm,
                    SlottedTupleId start, SlottedTupleId end)
    throws TransactionAbortException
  {
    validateTupleRange(start, end);
    return new BufferedSlottedFileIterator(engine, txn, perm, this, start, end);
  }

  // Returns a direct iterator over on-disk page headers.
  public SlottedFileHeaderIterator header_iterator() {
    return new SlottedFileHeaderIterator(this);
  }
  
  public HeaderFactory<SlottedPageHeader> getHeaderFactory() {
    return SlottedPage.headerFactory;
  }
}
