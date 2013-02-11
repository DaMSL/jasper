package edu.jhu.cs.damsl.engine.storage.iterator.file.buffered;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.engine.EngineException;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.accessor.BufferedPageAccessor;
import edu.jhu.cs.damsl.engine.storage.file.PAXHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PAXPage;
import edu.jhu.cs.damsl.engine.storage.page.PAXPageHeader;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class BufferedPAXFileIterator
                extends BufferedFileIterator<
                  PAXPageHeader, PAXPage, PAXHeapFile>
{
  public BufferedPAXFileIterator(
    StorageEngine<PAXPageHeader, PAXPage, PAXHeapFile> e,
    TransactionId t, Page.Permissions perm, PAXHeapFile f)
  {
    super(e, t, perm, f);
  }

  @Override
  public void nextValidTuple() {}
}
