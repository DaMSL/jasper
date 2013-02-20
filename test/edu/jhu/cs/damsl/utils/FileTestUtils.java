package edu.jhu.cs.damsl.utils;

import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.List;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.accessor.PageFileAccessor;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.SlottedStorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;

public class FileTestUtils
  extends CommonTestUtils<SlottedPageHeader, SlottedPage, SlottedHeapFile>
{

  protected SlottedHeapFile testFile;

  public FileTestUtils(boolean varSchema)
      throws FileNotFoundException
  {
    super(new SlottedStorageFileFactory());
    String fName = "test.dat";
    testFile = new SlottedHeapFile(
        storage, fName, pool.getPageSize(), Defaults.getDefaultFileSize(),
        varSchema? null : CommonTestUtils.getLIDSchema());
  }
  
  public SlottedHeapFile getFile() { return testFile; }

  public SlottedPage getSlottedPage(
      PageFileAccessor<SlottedPageHeader, SlottedPage> fileAccessor)
  {
    SlottedPage p = null;
    try { p = fileAccessor.getPage();
    } catch (InterruptedException e) { e.printStackTrace(); }
    
    assertTrue ( p != null );
    return p;
  }
  
  public List<SlottedPage> generateSlottedPages(
      PageFileAccessor<SlottedPageHeader, SlottedPage> pageAccessor,
      List<Tuple> tuples)
  {
    LinkedList<SlottedPage> r = new LinkedList<SlottedPage>();

    SlottedPage p = getSlottedPage(pageAccessor);
    r.add(p);

    for (Tuple t : tuples) {
      // Get a new page if the previous one is full.
      if ( p.getHeader().getFreeSpace() < t.size()) {
        p = getSlottedPage(pageAccessor);
        r.add(p);
      }
      
      // Put the generated data in the page.
      assertTrue( p.putTuple(t, (short) t.size()) );
    }
    return r;
  }

  public void writePages(List<SlottedPage> pages) {
    for (SlottedPage p : pages) {
      PageId newId = new PageId(testFile.fileId(), p.getId().pageNum());
      p.setId(newId);
      assertTrue ( testFile.writePage(p) == pool.getPageSize());
    }
  }
  
  public void writeTuples(
      PageFileAccessor<SlottedPageHeader, SlottedPage> pageAccessor,
      List<Tuple> tuples)
  {
    List<SlottedPage> pages = generateSlottedPages(pageAccessor, tuples);
    writePages(pages);
  }
  
  public void writeRandomTuples(
      PageFileAccessor<SlottedPageHeader, SlottedPage> pageAccessor)
  {
    List<Tuple> tuples = getTuples();
    writeTuples(pageAccessor, tuples);
  }

}
