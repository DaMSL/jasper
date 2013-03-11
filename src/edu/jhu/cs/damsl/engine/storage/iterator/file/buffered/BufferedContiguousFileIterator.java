package edu.jhu.cs.damsl.engine.storage.iterator.file.buffered;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.engine.EngineException;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.accessor.BufferedPageAccessor;
import edu.jhu.cs.damsl.engine.storage.file.ContiguousHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.Tuple;

public class BufferedContiguousFileIterator
                extends BufferedFileIterator<
                  ContiguousTupleId, PageHeader, ContiguousPage, ContiguousHeapFile>
{
  public BufferedContiguousFileIterator(
    StorageEngine<ContiguousTupleId, PageHeader, ContiguousPage, ContiguousHeapFile> e,
    TransactionId t, Page.Permissions perm, ContiguousHeapFile f)
  {
    super(e, t, perm, f);
  }

  public BufferedContiguousFileIterator(
    StorageEngine<ContiguousTupleId, PageHeader, ContiguousPage, ContiguousHeapFile> e,
    TransactionId t, Page.Permissions perm, ContiguousHeapFile f,
    ContiguousTupleId start, ContiguousTupleId end)
  {
    super(e, t, perm, f, start, end);
  }
}
