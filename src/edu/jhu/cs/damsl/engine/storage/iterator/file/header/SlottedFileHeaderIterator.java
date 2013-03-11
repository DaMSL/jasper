package edu.jhu.cs.damsl.engine.storage.iterator.file.header;

import java.util.Iterator;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.SlottedTupleId;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;

public class SlottedFileHeaderIterator extends
                StorageFileHeaderIterator<SlottedTupleId, SlottedPageHeader, SlottedPage>
{
  public SlottedFileHeaderIterator(
            StorageFile<SlottedTupleId, SlottedPageHeader, SlottedPage> f)
  {
    super(f);
  }
}
