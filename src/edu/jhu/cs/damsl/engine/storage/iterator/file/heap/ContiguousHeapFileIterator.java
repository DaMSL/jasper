package edu.jhu.cs.damsl.engine.storage.iterator.file.heap;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.EngineException;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.accessor.PageFileAccessor;
import edu.jhu.cs.damsl.engine.storage.file.ContiguousHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class ContiguousHeapFileIterator
                extends HeapFileIterator<PageHeader, ContiguousPage, ContiguousHeapFile>
{
  public ContiguousHeapFileIterator(DbBufferPool<PageHeader, ContiguousPage, ContiguousHeapFile> pool,
                                 ContiguousHeapFile file)
  {
    super(pool, file);
  }

  @Override
  public void nextValidTuple() {}

}
