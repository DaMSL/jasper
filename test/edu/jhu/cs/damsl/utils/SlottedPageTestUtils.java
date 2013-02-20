package edu.jhu.cs.damsl.utils;

import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.SlottedStorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;

public class SlottedPageTestUtils
  extends CommonTestUtils<SlottedPageHeader, SlottedPage, SlottedHeapFile>
{

  public SlottedPageTestUtils() {
    super(new SlottedStorageFileFactory());
  }
  
  // Slotted page construction.
  
  public SlottedPage getSlottedPage() {
    SlottedPage p = null;
    try { p = (SlottedPage) pool.getPage();
    } catch (InterruptedException e) { e.printStackTrace(); }
    
    assertTrue ( p != null );
    return p;
  }

  public List<SlottedPage> generateSlottedPages(List<Tuple> tuples) {
    LinkedList<SlottedPage> r = new LinkedList<SlottedPage>();

    SlottedPage p = null;
    for (Tuple t : tuples) {
      // Get a new page if the previous one is full.
      if ( p == null || p.getHeader().getFreeSpace() < t.size()) {
        p = getSlottedPage();
        r.add(p);
      }

      // Put the generated data in the page.
      assertTrue ( p.putTuple(t, (short) t.size()) );
    }
    
    return r;
  }
  
}
