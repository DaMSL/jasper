package edu.jhu.cs.damsl.engine.storage.iterator.file.heap;

import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.SlottedTupleId;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;

public class SlottedHeapFileIterator
                extends HeapFileIterator<TableId, SlottedTupleId, SlottedPageHeader,
                                         SlottedPage, SlottedHeapFile>
{
  public SlottedHeapFileIterator(DbBufferPool<TableId, SlottedTupleId, SlottedPageHeader,
                                              SlottedPage, SlottedHeapFile> pool,
                                 SlottedHeapFile file)
  {
    super(pool, file);
  }

  public SlottedHeapFileIterator(DbBufferPool<TableId, SlottedTupleId, SlottedPageHeader,
                                              SlottedPage, SlottedHeapFile> pool,
                                 SlottedHeapFile file,
                                 SlottedTupleId start,
                                 SlottedTupleId end)
  {
    super(pool, file, start, end);
  }
}
