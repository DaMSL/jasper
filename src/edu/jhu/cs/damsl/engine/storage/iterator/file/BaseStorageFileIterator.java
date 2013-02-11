package edu.jhu.cs.damsl.engine.storage.iterator.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.accessor.PageFileAccessor;
import edu.jhu.cs.damsl.engine.storage.iterator.page.PageIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public abstract class BaseStorageFileIterator<
                        HeaderType extends PageHeader,
                        PageType extends Page<HeaderType>>
                      implements StorageFileIterator<HeaderType, PageType>
{
  protected static final Logger logger = LoggerFactory.getLogger(StorageFileIterator.class);
  protected PageFileAccessor<HeaderType, PageType> paged;
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

  protected PageIterator<HeaderType, PageType> current;
  protected PageIterator<HeaderType, PageType> returned;

  public BaseStorageFileIterator(PageFileAccessor<HeaderType, PageType> rdr) {
    paged = rdr;
    fileId = rdr.getFileId();
    reset();
  }

  public void reset() {
    filePages = paged.getNumPages();
    returned = current = null;
    nextValidTuple();
  }

  public PageId nextPageId() {
    PageId r = null;
    if ( current == null ) { r = new PageId(fileId, 0); }
    else if ( current != null && current.getId().pageNum()+1 < filePages ) {
      r = new PageId(fileId, current.getId().pageNum()+1);
    }
    return r;
  }
  
  public abstract void nextValidTuple();

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
    if ( returned != null && returned.isReturnedValid() ) {
      returned.remove();
      returned = null;
    }
  }

  public PageFileAccessor<HeaderType, PageType> getAccessor() { return paged; } 

}
