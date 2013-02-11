package edu.jhu.cs.damsl.engine.storage.iterator.file.buffered;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
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
                  SlottedPageHeader, SlottedPage, SlottedHeapFile>
{
  public BufferedSlottedFileIterator(
    StorageEngine<SlottedPageHeader, SlottedPage, SlottedHeapFile> e,
    TransactionId t, Page.Permissions perm, SlottedHeapFile f)
  {
    super(e, t, perm, f);
  }

  @Override
  public void nextValidTuple() {
    PageId id = null;
    while ( (current == null || !current.hasNext())
              && (id = nextPageId()) != null )
    {
      SlottedPage p = null;
      try {
        p = paged.getPage(id);
      } catch (EngineException e) {
        logger.warn("could not read page {}", id);
      }

      current = (p == null? null : p.iterator());
      if ( current == null && p != null ) {
        // Invoke release before moving on to the next page. It is left
        // to the reader to determine what actually happens on release.
        paged.releasePage(p);
      }
    }
  }
}
