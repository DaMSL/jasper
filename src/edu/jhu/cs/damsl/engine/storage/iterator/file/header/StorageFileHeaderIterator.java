package edu.jhu.cs.damsl.engine.storage.iterator.file.header;

import java.util.Iterator;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public interface StorageFileHeaderIterator<
                    HeaderType extends PageHeader> 
                  extends Iterator<HeaderType>
{
  public FileId getFileId();
  public PageId getPageId();

  void nextPageId();

  public void reset();
}