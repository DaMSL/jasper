package edu.jhu.cs.damsl.engine.storage.iterator.file.buffered;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.engine.EngineException;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.accessor.BufferedPageAccessor;
import edu.jhu.cs.damsl.engine.storage.file.ContiguousHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class BufferedContiguousFileIterator
                extends BufferedFileIterator<
                  PageHeader, ContiguousPage, ContiguousHeapFile>
{
  public BufferedContiguousFileIterator(
    StorageEngine<PageHeader, ContiguousPage, ContiguousHeapFile> e,
    TransactionId t, Page.Permissions perm, ContiguousHeapFile f)
  {
    super(e, t, perm, f);
  }

  @Override
  public void nextValidTuple() {}
}
