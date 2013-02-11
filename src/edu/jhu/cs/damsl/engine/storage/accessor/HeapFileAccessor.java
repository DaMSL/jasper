package edu.jhu.cs.damsl.engine.storage.accessor;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.file.HeapFile;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public class HeapFileAccessor<
                  HeaderType extends PageHeader,
                  PageType extends Page<HeaderType>,
                  FileType extends StorageFile<HeaderType, PageType>>
                implements PageFileAccessor<HeaderType, PageType> {
  
  DbBufferPool<HeaderType, PageType, FileType> pool;
  FileType file;
  PageType currentPage;

  public HeapFileAccessor(DbBufferPool<HeaderType, PageType, FileType> p, FileType f)
  {
    pool = p;
    file = f; 
    currentPage = null;
  }
  
  void initializeCurrentPage() {
    try {
      currentPage = pool.getPage();
    } catch (InterruptedException e) { e.printStackTrace(); }
  }
  
  @Override
  public FileType getFile() { return file; };

  @Override
  public FileId getFileId() { return file.fileId(); }

  @Override
  public Integer getPageSize() { return pool.getPageSize(); }

  @Override
  public int getNumPages() { return file.numPages(); }

  @Override
  public PageType getPage() {
    if ( currentPage == null ) { initializeCurrentPage(); }
    return currentPage;
  }
  
  @Override
  public PageType getPage(PageId id) {
    if ( currentPage == null ) { initializeCurrentPage(); }
    if ( currentPage != null ) { file.readPage(currentPage, id); }
    return currentPage;
  }
  
  @Override
  public void releasePage(PageType p) {
    if ( currentPage == p ) {
      pool.releasePage(p);
      currentPage = null;
    }
  }
  
}
