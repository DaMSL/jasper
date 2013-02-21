package edu.jhu.cs.damsl.utils;

import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.List;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.accessor.PageFileAccessor;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.StorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public class FileTestUtils<HeaderType extends PageHeader,
                           PageType   extends Page<HeaderType>,
                           FileType   extends StorageFile<HeaderType, PageType>>
  extends CommonTestUtils<HeaderType, PageType, FileType>
{

  protected FileType testFile;

  public FileTestUtils(StorageFileFactory<HeaderType, PageType, FileType> factory,
                       boolean varSchema)
      throws FileNotFoundException
  {
    super(factory);
    String fName = "test.dat";
    testFile = factory.getFile(fName, varSchema? null : CommonTestUtils.getLIDSchema());
  }
  
  public FileType getFile() { return testFile; }

  public PageType getPage(PageFileAccessor<HeaderType, PageType> fileAccessor)
  {
    PageType p = null;
    try { p = fileAccessor.getPage();
    } catch (InterruptedException e) { e.printStackTrace(); }
    
    assertTrue ( p != null );
    return p;
  }
  
  public List<PageType> generatePages(
                          PageFileAccessor<HeaderType, PageType> pageAccessor,
                          List<Tuple> tuples)
  {
    LinkedList<PageType> r = new LinkedList<PageType>();

    PageType p = getPage(pageAccessor);
    r.add(p);

    for (Tuple t : tuples) {
      // Get a new page if the previous one is full.
      if ( p.getHeader().getFreeSpace() < t.size()) {
        p = getPage(pageAccessor);
        r.add(p);
      }
      
      // Put the generated data in the page.
      assertTrue( p.putTuple(t, (short) t.size()) );
    }
    return r;
  }

  public void writePages(List<PageType> pages) {
    for (PageType p : pages) {
      PageId newId = new PageId(testFile.fileId(), p.getId().pageNum());
      p.setId(newId);
      assertTrue ( testFile.writePage(p) == getPool().getPageSize());
    }
  }
  
  public void writeTuples(PageFileAccessor<HeaderType, PageType> pageAccessor,
                          List<Tuple> tuples)
  {
    List<PageType> pages = generatePages(pageAccessor, tuples);
    writePages(pages);
  }
  
  public void writeRandomTuples(PageFileAccessor<HeaderType, PageType> pageAccessor)
  {
    List<Tuple> tuples = getTuples();
    writeTuples(pageAccessor, tuples);
  }

}
