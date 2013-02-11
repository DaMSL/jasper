package edu.jhu.cs.damsl.engine.storage.file;

import java.io.FileNotFoundException;
import java.io.IOException;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.engine.storage.iterator.file.StorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.StorageFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class ContiguousHeapFile extends HeapFile<PageHeader, ContiguousPage> {

  public ContiguousHeapFile(String fName, Integer pageSize, Long capacity)
      throws FileNotFoundException
  {
    this(fName, pageSize, capacity, null);
  }

  public ContiguousHeapFile(String fName, Integer pageSz, Long capacity, Schema sch)
      throws FileNotFoundException
  {
    super(fName, pageSz, capacity, sch);
  }

  public ContiguousHeapFile(FileId id, Schema sch, TableId rel)
      throws FileNotFoundException
  {
    super(id, sch, rel);
  }

  @CS416Todo
  public StorageIterator iterator() { return null; }

  @CS416Todo
  public StorageFileIterator<PageHeader, ContiguousPage> heap_iterator() { 
    return null;
  }

  @CS416Todo
  public StorageFileIterator<PageHeader, ContiguousPage>
  buffered_iterator(TransactionId txn, Page.Permissions perm) { 
    return null;
  }


  // Returns a direct iterator over on-disk page headers.
  @CS416Todo
  public StorageFileHeaderIterator<PageHeader> header_iterator() {
    return null;
  }
  
  @CS416Todo
  public HeaderFactory<PageHeader> getHeaderFactory() {
    return null;
  }
}