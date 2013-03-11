package edu.jhu.cs.damsl.engine.storage.index;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.FileId.FileKind;
import edu.jhu.cs.damsl.catalog.identifiers.IndexId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.file.HeapFile;
import edu.jhu.cs.damsl.engine.storage.iterator.file.StorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.StorageFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.index.IndexIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.index.IndexEntryIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.index.IndexFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.index.IndexEntryFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.transactions.TransactionAbortException;
import edu.jhu.cs.damsl.factory.page.HeaderFactory;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;
import edu.jhu.cs.damsl.utils.hw2.HW2.*;


@CS316Todo(methods = "getRoot, getFirstLeafPage")
@CS416Todo(methods = "getRoot, getFirstLeafPage")
public class IndexFile<IdType extends TupleId>
                extends HeapFile<ContiguousTupleId, PageHeader, IndexPage<IdType>>
{
  IndexId indexId;
  Schema indexKeySchema;

  DbBufferPool<IndexId, ContiguousTupleId,
               PageHeader, IndexPage<IdType>, IndexFile<IdType>> indexPagePool;
  TupleIdFactory<IdType> factory;
  
  public IndexFile(DbBufferPool<IndexId, ContiguousTupleId,
                      PageHeader, IndexPage<IdType>, IndexFile<IdType>> pool,
                   TupleIdFactory<IdType> f, IndexId id, Schema key, 
                   FileKind k, Integer pageSize, Long capacity)
    throws FileNotFoundException
  {
    super(k, pageSize, capacity);
    indexId = id;
    indexKeySchema = key;
    indexPagePool = pool;
    factory = f;
  }

  public IndexId getIndexId() { return indexId; }
  public Schema getIndexSchema() { return indexKeySchema; }

  public HeaderFactory<PageHeader> getHeaderFactory() {
    return ContiguousPage.headerFactory;
  }

  // Returns the page corresponding to the root of the index.
  // This is the start point for all traversals.
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public PageId getRoot() {
    throw new UnsupportedOperationException();
  }

  // Returns the page corresponding to the first leaf page of the index.
  // This can be used to scan the leaf layer alone.
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public PageId getFirstLeafPage() {
    throw new UnsupportedOperationException();
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

  // Iterators over index files and index pages.

  // An iterator over the index's leaf pages, which contains
  // search keys in sorted order and tuple ids. 
  public IndexIterator<IdType> index_iterator() {
    return new IndexFileIterator<IdType>(indexPagePool, factory, this);
  }

  public IndexIterator<IdType>
  index_iterator(ContiguousTupleId start, ContiguousTupleId end)
    throws TransactionAbortException
  {
    validateTupleRange(start, end);
    return new IndexFileIterator<IdType>(indexPagePool, factory, this, start, end);
  }

  // An iterator over all index entries, which includes non-leaf and leaf entries.
  public IndexEntryIterator<IdType> entry_iterator() {
    return new IndexEntryFileIterator<IdType>(indexPagePool, factory, this);
  }

  // Abstract method implementations from HeapFile.
  // No need to use or implement these for Assignment 2.

  public StorageIterator iterator() {
    throw new UnsupportedOperationException();
  }

  public StorageIterator iterator(ContiguousTupleId start, ContiguousTupleId end)
    throws TransactionAbortException
  {
    validateTupleRange(start, end);
    throw new UnsupportedOperationException();
  }

  public StorageFileIterator<ContiguousTupleId, PageHeader, IndexPage<IdType>>
  heap_iterator() { 
    throw new UnsupportedOperationException();
  }

  public StorageFileIterator<ContiguousTupleId, PageHeader, IndexPage<IdType>>
  heap_iterator(ContiguousTupleId start, ContiguousTupleId end)
    throws TransactionAbortException
  { 
    validateTupleRange(start, end);
    throw new UnsupportedOperationException();
  }

  public StorageFileIterator<ContiguousTupleId, PageHeader, IndexPage<IdType>>
  buffered_iterator(TransactionId txn, Page.Permissions perm) { 
    throw new UnsupportedOperationException();
  }

  public StorageFileIterator<ContiguousTupleId, PageHeader, IndexPage<IdType>>
  buffered_iterator(TransactionId txn, Page.Permissions perm,
                    ContiguousTupleId start, ContiguousTupleId end)
    throws TransactionAbortException
  { 
    validateTupleRange(start, end);
    throw new UnsupportedOperationException();
  }

  // Returns a direct iterator over on-disk page headers.
  public StorageFileHeaderIterator<ContiguousTupleId, PageHeader, IndexPage<IdType>>
  header_iterator() {
    throw new UnsupportedOperationException();
  }
}
