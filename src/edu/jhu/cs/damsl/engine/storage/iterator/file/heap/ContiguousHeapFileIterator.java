package edu.jhu.cs.damsl.engine.storage.iterator.file.heap;

import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.EngineException;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.accessor.PageFileAccessor;
import edu.jhu.cs.damsl.engine.storage.file.ContiguousHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public class ContiguousHeapFileIterator
                extends HeapFileIterator<TableId, ContiguousTupleId, PageHeader,
                						             ContiguousPage, ContiguousHeapFile>
{
  public ContiguousHeapFileIterator(DbBufferPool<TableId, ContiguousTupleId, PageHeader,
                                                 ContiguousPage, ContiguousHeapFile> pool,
                                    ContiguousHeapFile file)
  {
    super(pool, file);
  }

  public ContiguousHeapFileIterator(DbBufferPool<TableId, ContiguousTupleId, PageHeader,
                                                 ContiguousPage, ContiguousHeapFile> pool,
                                    ContiguousHeapFile file,
                                    ContiguousTupleId start,
                                    ContiguousTupleId end)
  {
    super(pool, file, start, end);
  }

}
