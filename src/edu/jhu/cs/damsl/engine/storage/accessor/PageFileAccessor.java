package edu.jhu.cs.damsl.engine.storage.accessor;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public interface PageFileAccessor<
                      IdType extends TupleId,
                      HeaderType extends PageHeader,
                      PageType extends Page<IdType, HeaderType>>
                   extends PageAccessor<IdType, HeaderType, PageType>
{
  public StorageFile<IdType, HeaderType, PageType> getFile();
  public FileId getFileId();
  public int getNumPages();
}
