package edu.jhu.cs.damsl.engine.storage.accessor;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public interface PageFileAccessor<
                      HeaderType extends PageHeader,
                      PageType extends Page<HeaderType>>
                   extends PageAccessor<HeaderType, PageType>
{
  public StorageFile<HeaderType, PageType> getFile();
  public FileId getFileId();
  public int getNumPages();
}
