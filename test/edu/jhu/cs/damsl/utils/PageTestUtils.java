package edu.jhu.cs.damsl.utils;

import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.StorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public class PageTestUtils<HeaderType extends PageHeader,
                           PageType   extends Page<HeaderType>,
                           FileType   extends StorageFile<HeaderType, PageType>>
  extends CommonTestUtils<HeaderType, PageType, FileType>
{

  StorageFileFactory<HeaderType, PageType, FileType> factory;
  
  public PageTestUtils(StorageFileFactory<HeaderType, PageType, FileType> f) {
    super(f);
    factory = f;
  }
  
  // Page construction.
  
  public PageType getPage() {
    PageType p = null;
    try { 
      PageType tmp = getPool().getPage();
      p = factory.getPageFactory().getPage(tmp.getId().pageNum(), tmp);
    } catch (InterruptedException e) { e.printStackTrace(); }
    
    assertTrue ( p != null );
    return p;
  }

  public List<PageType> generatePages(List<Tuple> tuples) {
    LinkedList<PageType> r = new LinkedList<PageType>();

    PageType p = null;
    for (Tuple t : tuples) {
      // Get a new page if the previous one is full.
      if ( p == null || p.getHeader().getFreeSpace() < t.size()) {
        p = getPage();
        r.add(p);
      }

      // Put the generated data in the page.
      assertTrue ( p.putTuple(t, (short) t.size()) );
    }
    
    return r;
  }
  
}
