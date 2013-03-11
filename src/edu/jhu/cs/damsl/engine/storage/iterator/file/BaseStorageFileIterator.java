package edu.jhu.cs.damsl.engine.storage.iterator.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.EngineException;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.accessor.PageFileAccessor;
import edu.jhu.cs.damsl.engine.storage.iterator.page.PageIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public abstract class BaseStorageFileIterator<
                        IdType extends TupleId,
                        HeaderType extends PageHeader,
                        PageType extends Page<IdType, HeaderType>>
                      implements StorageFileIterator<IdType, HeaderType, PageType>
{
  protected static final Logger logger = LoggerFactory.getLogger(StorageFileIterator.class);
  protected PageFileAccessor<IdType, HeaderType, PageType> paged;
  protected FileId fileId;

  // Locally cached copy of the number of pages in the file. Thus, this iterator
  // will not see new pages added by other transactions during its lifetime.
  // Iterator construction must occur in locked fashion, i.e. while holding a
  // read lock on the table. This is only for iterator construction, not for
  // traversal.
  // Page removal due to file compaction must also occur in locked fashion,
  // while holding a write lock on the table. Currently this DBMS does not 
  // support compaction.
  protected int filePages;

  protected PageIterator<IdType, HeaderType, PageType> current;
  protected PageIterator<IdType, HeaderType, PageType> returned;

  protected IdType start, end;

  public BaseStorageFileIterator(
            PageFileAccessor<IdType, HeaderType, PageType> rdr)
  {
    paged = rdr;
    fileId = rdr.getFileId();
    start = end = null;
    reset();
  }

  public BaseStorageFileIterator(
            PageFileAccessor<IdType, HeaderType, PageType> rdr,
            IdType start, IdType end)
  {
    paged = rdr;
    fileId = rdr.getFileId();
    this.start = start;
    this.end = end;
    reset();
  }

  public void reset() {
    filePages = paged.getNumPages();
    returned = current = null;
    nextValidTuple();
  }

  public PageId nextPageId() {
    PageId r = null;
    if ( current == null ) { 
      r = start == null ? new PageId(fileId, 0) : start.pageId();
    } 
    // TODO change to PageId.equals()
    // Move on to the next page only if we are not at the desired end
    // and pages remain in the file.
    else if ( (end != null && current.getPageId() != end.pageId())
              && (current.getPageId().pageNum()+1 < filePages) )
    {
      r = new PageId(fileId, current.getPageId().pageNum()+1);
    }
    return r;
  }
  
  @SuppressWarnings("unchecked")
  public void nextValidTuple() {
    PageId id = null;
    while ( (current == null || !current.hasNext())
              && (id = nextPageId()) != null )
    {
      PageType p = null;
      try {
        p = paged.getPage(id);
      } catch (EngineException e) {
        logger.warn("could not read page {}", id);
      }

      if ( p == null ) { current = null; }
      else if ( start == null && end == null ) { 
        current = (PageIterator<IdType, HeaderType, PageType>) p.iterator();
      } else {
        // TODO: change to PageId.equals()
        IdType s = id == start.pageId()? start : null;
        IdType e = id == end.pageId()? end : null;
        current = (PageIterator<IdType, HeaderType, PageType>) p.iterator(s, e);
      }
      
      if ( current == null && p != null ) {
        // Invoke release before moving on to the next page. It is left
        // to the reader to determine what actually happens on release.
        paged.releasePage(p);
      }
    }
  }

  public void markReturned() { returned = current; }

  public boolean hasNext() {
    return current != null && current.hasNext();
  }

  public Tuple next() {
    Tuple r = null;
    if ( current != null && current.hasNext() ) {
      r = current.next();
      markReturned();
      nextValidTuple();
    }
    return r;
  }

  public void remove() {
    // Currently, we cannot remove tuples via this iterator if it
    // has a desired endpoint. This would require updating the endpoint
    // on removals.
    if ( end != null ) { throw new UnsupportedOperationException(); }

    if ( returned != null && returned.isReturnedValid() ) {
      returned.remove();
      returned = null;
    }
  }

  public PageFileAccessor<IdType, HeaderType, PageType> getAccessor() { return paged; } 

}
