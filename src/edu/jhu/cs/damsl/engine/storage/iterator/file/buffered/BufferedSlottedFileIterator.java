package edu.jhu.cs.damsl.engine.storage.iterator.file.buffered;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.SlottedTupleId;
import edu.jhu.cs.damsl.engine.EngineException;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.accessor.BufferedPageAccessor;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.Tuple;

public class BufferedSlottedFileIterator
                extends BufferedFileIterator<
                  SlottedTupleId, SlottedPageHeader, SlottedPage, SlottedHeapFile>
{
  public BufferedSlottedFileIterator(
    StorageEngine<SlottedTupleId, SlottedPageHeader, SlottedPage, SlottedHeapFile> e,
    TransactionId t, Page.Permissions perm, SlottedHeapFile f)
  {
    super(e, t, perm, f);
  }

  public BufferedSlottedFileIterator(
    StorageEngine<SlottedTupleId, SlottedPageHeader, SlottedPage, SlottedHeapFile> e,
    TransactionId t, Page.Permissions perm, SlottedHeapFile f,
    SlottedTupleId start, SlottedTupleId end)
  {
    super(e, t, perm, f, start, end);
  }
}
