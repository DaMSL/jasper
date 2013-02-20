package edu.jhu.cs.damsl.utils;

import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.file.ContiguousHeapFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.ContiguousStorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public class ContiguousPageTestUtils
  extends CommonTestUtils<PageHeader, ContiguousPage, ContiguousHeapFile>
{

  public ContiguousPageTestUtils() {
    super(new ContiguousStorageFileFactory());
  }

  // Contiguous page construction.

  public ContiguousPage getContiguousPage(Schema s, boolean fillBackward) {
    ContiguousPage p = null;
    try {
      Page tmp = pool.getPage();
      byte flags = fillBackward? PageHeader.FILL_BACKWARD : (byte) 0x0;
      p = new ContiguousPage(tmp.getId().pageNum(), tmp, s, flags);
    } catch (InterruptedException e) { e.printStackTrace(); }
    
    assertTrue ( p != null );
    return p;
  }

  public List<ContiguousPage> generateContiguousPages(
      boolean fillBackward, Schema sch, List<Tuple> tuples)
  {
    LinkedList<ContiguousPage> r = new LinkedList<ContiguousPage>();

    ContiguousPage p = null;
    for (Tuple t : tuples) {
      // Get a new page if the previous one is full.
      if ( p == null || p.getHeader().getFreeSpace() < t.size()) {
        p = getContiguousPage(sch, fillBackward);
        r.add(p);
      }
      
      // Put the generated data in the page.
      assertTrue ( p.putTuple(t) );
    }
    
    return r;
  }
  
}
