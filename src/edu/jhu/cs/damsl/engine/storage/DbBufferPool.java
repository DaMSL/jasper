package edu.jhu.cs.damsl.engine.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Defaults.SizeUnits;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.engine.storage.BaseBufferPool;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.utils.LRUCache;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS316Todo
@CS416Todo
public class DbBufferPool<HeaderType extends PageHeader,
                          PageType   extends Page<HeaderType>,
                          FileType   extends StorageFile<HeaderType, PageType>>
                extends BaseBufferPool<HeaderType, PageType>
{
  // Page cache, tracking used buffer pool pages in access order.
  LinkedHashMap<PageId, PageType> pageCache;
  
  StorageEngine<HeaderType, PageType, FileType> storage;

  // Buffer pool statistics.
  long numRequests;
  long numHits;
  long numEvictions;
  long numFailedEvictions;

  public DbBufferPool(StorageEngine<HeaderType, PageType, FileType> e,
                      Integer bufferSize, SizeUnits bufferUnit,
                      Integer pageSize, SizeUnits pageUnit)
  {
    super(e.getPageFactory(), bufferSize, bufferUnit, pageSize, pageUnit);
    storage = e;
    pageCache = new LinkedHashMap<PageId, PageType>();
    numRequests = numHits = numEvictions = numFailedEvictions = 0;
  }

  /**
   * Cached page access.
   * This should check if the page is already in the buffer pool, 
   * and issue a read request to the file manager to pull the page into the
   * buffer pool as necessary. Pages should be evicted as needed if the
   * cache is full.
   *
   * @param id the page id of the page being requested
   * @return an in-memory page reflecting the contents of the on-disk page
   */
  @CS316Todo
  @CS416Todo
  public PageType getPage(PageId id) {
    return null;
  }

  /**
   * Return a cached page to the free list without flushing it, regardless
   * of whether the page is dirty.
   *
   * @param id the page to flush
   */
  @CS316Todo
  @CS416Todo
  public void discardPage(PageId id) {}

  /**
   * Cache eviction policy, yielding the page that has just been evicted for
   * reuse, without putting it back on the free page list.
   *
   * @return the page evicted under the replacement policy
   */
  @CS316Todo
  @CS416Todo
  PageType evictPage() {
    return null;
  }
  
  /**
    * Flush a cached page to disk if it is dirty, returning it to the free list.
    */
  @CS316Todo
  @CS416Todo
  public void flushPage(PageId id) {}
  
  /**
    * Flush all dirty pages currently in the buffer pool to disk.
    */
  @CS316Todo
  @CS416Todo
  public void flushPages() {}

  /**
    * Buffered data access for adding data to the given relation.
    * This method should support requests for a page with at least
    * <code>requestedSpace</code> bytes free. If no such page is already
    * in the buffer pool, a request should be made on files backing
    * the relation in the file manager. The file manager may need to
    * extend its current files, and any page returned should be added
    * to the buffer pool.
    *
    * @param rel the relation for which we are requesting a page with
    *            sufficient free space
    * @param requestedSpace the amount of space requested
    * @return the page id of a page satisfying the space requirements
    */
  @CS316Todo
  @CS416Todo
  public PageId getWriteablePage(TableId rel, short requestedSpace) {
    return null;
  }

  public String toString() {
    double hitRate = Long.valueOf(numHits).doubleValue() / Long.valueOf(numRequests).doubleValue();

    String r = "hits: "              + (numRequests > 0? Double.toString(hitRate) : "<>") +
               " requests: "         + Long.toString(numRequests) +
               " hits: "             + Long.toString(numHits) +
               " evictions: "        + Long.toString(numEvictions) +
               " failed evictions: " + Long.toString(numFailedEvictions) + "\n";

    r += "free pages: " + Integer.toString(getNumFreePages()) +
         " total pages: " + Integer.toString(getNumPages());

    return r;
  }

}
