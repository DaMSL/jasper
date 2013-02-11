package edu.jhu.cs.damsl.engine.storage.iterator.page;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public abstract class PageIterator<
                          HeaderType extends PageHeader,
                          PageType extends Page<HeaderType>>
                        implements StorageIterator
{
  protected PageId currentPageId;
  protected PageType currentPage;

  public PageIterator(PageId id, PageType p) {
    currentPageId = id;
    currentPage = p;
  }

  public PageId getId() { return currentPageId; }
  
  public PageType getPage() { return currentPage; }

  public abstract boolean isCurrentValid();
  
  public abstract boolean isReturnedValid();
}
