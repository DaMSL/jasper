package edu.jhu.cs.damsl.engine.storage.iterator.file.header;

import java.util.Iterator;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public class ContiguousFileHeaderIterator extends
                StorageFileHeaderIterator<ContiguousTupleId, PageHeader, ContiguousPage>
{
  public ContiguousFileHeaderIterator(
            StorageFile<ContiguousTupleId, PageHeader, ContiguousPage> f)
  {
    super(f);
  }
}
