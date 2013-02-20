package edu.jhu.cs.damsl.engine.storage.file;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.accessor.BufferedPageAccessor;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.Page.Permissions;
import edu.jhu.cs.damsl.utils.FileTestUtils;

public class SlottedHeapFileTest {

  private FileTestUtils ftUtils;
  private BufferedPageAccessor<SlottedPageHeader, SlottedPage, SlottedHeapFile> fileAccessor;

  @Before
  public void setUp() throws Exception {
    ftUtils = new FileTestUtils(true);
    fileAccessor =
      new BufferedPageAccessor<SlottedPageHeader, SlottedPage, SlottedHeapFile>(
        ftUtils.getStorage(), null, Permissions.WRITE, ftUtils.getFile());
  }

  @Test
  public void writeTest() {
    List<Tuple> tuples = ftUtils.getTuples();
    List<SlottedPage> pages = ftUtils.generateSlottedPages(fileAccessor, tuples);

    long len = 0;
    for (SlottedPage p : pages) {
      PageId newId = new PageId(fileAccessor.getFileId(), p.getId().pageNum());
      p.setId(newId);
      assertTrue( fileAccessor.getFile().writePage(p)
                        == ftUtils.getPool().getPageSize()
                    && fileAccessor.getFile().size() > len );
      len = fileAccessor.getFile().size();
    }
    
    // Write pages again, and ensure no change in file size.
    for (SlottedPage p : pages) {
      PageId newId = new PageId(fileAccessor.getFileId(), p.getId().pageNum());
      p.setId(newId);
      assertTrue( fileAccessor.getFile().writePage(p)
                        == ftUtils.getPool().getPageSize()
                    && fileAccessor.getFile().size() == len );
    }
  }
  
  @Test
  public void readTest() {
    ftUtils.writeRandomTuples(fileAccessor);

    PageId id = new PageId(fileAccessor.getFileId(),0);
    PageId dummyId = new PageId(new FileId("dummy.dat"),0);
    
    SlottedPage p = ftUtils.getSlottedPage(fileAccessor);

    p.setId(dummyId);
    assertTrue( fileAccessor.getFile().readPage(p, id)
                  == ftUtils.getPool().getPageSize() );
    assertTrue( p.getId().equals(id) );
  }

}
