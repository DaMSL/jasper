package edu.jhu.cs.damsl.engine.storage.accessor;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.EngineException;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public interface PageAccessor<
                    HeaderType extends PageHeader,
                    PageType extends Page<HeaderType>> 
{
  public Integer getPageSize();
  public PageType getPage() throws InterruptedException;
  public PageType getPage(PageId p) throws EngineException;
  public void releasePage(PageType p);
}
