package edu.jhu.cs.damsl.engine.storage;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingDeque;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.DirectChannelBufferFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Defaults.SizeUnits;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.accessor.PageAccessor;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageFactory;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

/**
 * Manages the caching of pages in memory.
 *
 * A Buffer Pool manages the transfer of pages between disk and memory, and vice-versa. It also
 * handles the caching policies used, with the objective of minimizing the number of disk
 * operations. These policies include eviction strategies for removing pages from memory, and
 * prefetching strategies, for pre-emptively reading pages from disk.
 */
public abstract class BaseBufferPool<
                          HeaderType extends PageHeader,
                          PageType   extends Page<HeaderType>>
                        implements PageAccessor<HeaderType, PageType>
{  
  protected static final Logger logger = LoggerFactory.getLogger(BaseBufferPool.class);
  protected DirectChannelBufferFactory pool;
  
  protected LinkedBlockingDeque<PageType> freePages;
  protected Integer pageSize;
  protected Integer numPages;

  public BaseBufferPool(PageFactory<HeaderType, PageType> pageFactory,
                        Integer bufferSize, SizeUnits bufferUnit,
                        Integer pageSize, SizeUnits pageUnit)
  {
    Integer actualBufSize = Defaults.getSizeAsInteger(bufferSize, bufferUnit);
    Integer actualPageSize = Defaults.getSizeAsInteger(pageSize, pageUnit);
    
    logger.info("buffer pool: {}, page size: {}", actualBufSize, actualPageSize);
    
    pool = new DirectChannelBufferFactory(actualBufSize);
    freePages = new LinkedBlockingDeque<PageType>();
    
    numPages = actualBufSize / actualPageSize;
    this.pageSize = actualPageSize;
    logger.info("wasted buffer pool: "+(actualBufSize % actualPageSize));
    
    initPages(pageFactory);
  }
  
  public Integer getPageSize() { return pageSize; }

  public Integer getNumPages() { return numPages; }
  
  public Integer getNumFreePages() { return freePages.size(); }

  protected void initPages(PageFactory<HeaderType, PageType> pageFactory) {
    for (int i = 0; i < numPages; ++i) {
      ChannelBuffer buf = pool.getBuffer(pageSize);
      if ( buf != null ) freePages.add(pageFactory.getPage(i, buf, null));
    }
  }
  
  protected int transferPages(Collection<Page> target, int count) {
    return freePages.drainTo(target, count);
  }

  public PageType getPage() throws InterruptedException {
    return freePages.take();
  }

  public PageType getPageIfReady() { return freePages.poll(); }
  
  public void releasePage(PageType p) {
    try {
      // Reset the page id based on the state of the free list.
      p.setId(new PageId(getNumFreePages()));
      freePages.put(p);
    } catch (InterruptedException e) {
      logger.error("interrupted while waiting for space on the free list");
      logger.error("phantom page: {}", p.getId());
      --numPages;
    }    
  }

}
