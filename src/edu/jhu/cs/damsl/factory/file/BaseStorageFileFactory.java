package edu.jhu.cs.damsl.factory.file;

import java.io.FileNotFoundException;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.FileId.FileKind;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.factory.index.IndexFileFactory;
import edu.jhu.cs.damsl.factory.index.ContiguousIndexFileFactory;
import edu.jhu.cs.damsl.factory.page.PageFactory;

public abstract class BaseStorageFileFactory<
                      IdType      extends TupleId,
                      HeaderType  extends PageHeader,
                      PageType    extends Page<IdType, HeaderType>,
                      FileType    extends StorageFile<IdType, HeaderType, PageType>>
        implements StorageFileFactory<IdType, HeaderType, PageType, FileType>
{
  public final IndexFileFactory<IdType> idxFactory =
    new ContiguousIndexFileFactory<IdType>();

  StorageEngine<IdType, HeaderType, PageType, FileType> engine;
  Integer pageSize;
  Long capacity;

  public BaseStorageFileFactory() {}

  public void initialize(DbEngine<IdType, HeaderType, PageType, FileType> dbms)
  {
    engine   = dbms.getStorageEngine();
    pageSize = engine.getBufferPool().getPageSize();
    capacity = Defaults.getDefaultFileSize();
  }

  public Integer getPageSize() { return pageSize; }

  public abstract PageFactory<IdType, HeaderType, PageType> getPageFactory();

  public IndexFileFactory<IdType> getIndexFileFactory() { return idxFactory; }

  public abstract FileType getFile(FileKind k) throws FileNotFoundException;

  public abstract FileType getFile(FileKind k, Schema sch)
    throws FileNotFoundException;

  public abstract FileType getFile(FileId id, Schema sch, TableId rel)
    throws FileNotFoundException;

}