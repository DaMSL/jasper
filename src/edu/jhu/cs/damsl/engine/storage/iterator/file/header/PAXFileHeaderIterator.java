package edu.jhu.cs.damsl.engine.storage.iterator.file.header;

import java.util.Iterator;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.PAXPage;
import edu.jhu.cs.damsl.engine.storage.page.PAXPageHeader;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class PAXFileHeaderIterator
              implements StorageFileHeaderIterator<PAXPageHeader>
{  
  public PAXFileHeaderIterator(StorageFile<PAXPageHeader, PAXPage> f) {}
  
  public FileId getFileId() { return null; }

  public PageId getPageId() { return null; }
  
  public void reset() {}
  
  public void nextPageId() {}

  public boolean hasNext() { return false; }

  public PAXPageHeader next() { return null; }

  public void remove() {}

}
