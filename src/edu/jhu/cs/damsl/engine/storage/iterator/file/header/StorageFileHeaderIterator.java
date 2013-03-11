package edu.jhu.cs.damsl.engine.storage.iterator.file.header;

import java.util.Iterator;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public abstract class StorageFileHeaderIterator<
                          IdType         extends TupleId,
                          HeaderType     extends PageHeader,
                          PageType       extends Page<IdType, HeaderType>>
                        implements Iterator<HeaderType>
{
  StorageFile<IdType, HeaderType, PageType> file;
  FileId fileId;
  int filePages;
  PageId currentPageId;

  public StorageFileHeaderIterator(StorageFile<IdType, HeaderType, PageType> f)
  {
    file = f;
    fileId = f.fileId();
    reset();
  }
  
  public FileId getFileId() { return fileId; }

  public PageId getPageId() { return currentPageId; }
  
  public void reset() {
    filePages = file.numPages();
    currentPageId = ( filePages > 0? new PageId(fileId, 0) : null );
  }
  
  public void nextPageId() {
    if ( currentPageId != null ) {
      currentPageId = ( currentPageId.pageNum()+1 < filePages?
          new PageId(fileId, currentPageId.pageNum()+1) : null );
    }
  }

  public boolean hasNext() {
    return ( currentPageId == null ?
        false : currentPageId.pageNum() < filePages);
  }

  public HeaderType next() {
    HeaderType r = null;
    if ( currentPageId == null ) return r;
    r = file.readPageHeader(currentPageId);
    nextPageId();
    return r;
  }

  public void remove() {
    throw new UnsupportedOperationException("cannot remove page headers");
  }

}