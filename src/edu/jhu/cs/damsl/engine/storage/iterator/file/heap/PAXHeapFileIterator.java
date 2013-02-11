package edu.jhu.cs.damsl.engine.storage.iterator.file.heap;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.EngineException;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.accessor.PageFileAccessor;
import edu.jhu.cs.damsl.engine.storage.file.PAXHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.PAXPage;
import edu.jhu.cs.damsl.engine.storage.page.PAXPageHeader;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class PAXHeapFileIterator
                extends HeapFileIterator<PAXPageHeader, PAXPage, PAXHeapFile>
{
  public PAXHeapFileIterator(DbBufferPool<PAXPageHeader, PAXPage, PAXHeapFile> pool,
                             PAXHeapFile file)
  {
    super(pool, file);
  }

  @Override
  public void nextValidTuple() {}

}
