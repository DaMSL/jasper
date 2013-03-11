package edu.jhu.cs.damsl.engine.storage.iterator.page;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public abstract class PageIterator<
                          IdType extends TupleId,
                          HeaderType extends PageHeader,
                          PageType extends Page<IdType, HeaderType>>
                        implements StorageIterator
{
  protected IdType currentTupleId;
  protected PageId currentPageId;
  protected PageType currentPage;

  public PageIterator(PageId id, PageType p) {
    currentTupleId = null;
    currentPageId = id;
    currentPage = p;
  }

  public IdType getTupleId() { return currentTupleId; }

  public PageId getPageId() { return currentPageId; }
  
  public PageType getPage() { return currentPage; }

  public abstract boolean isCurrentValid();
  
  public abstract boolean isReturnedValid();
}
