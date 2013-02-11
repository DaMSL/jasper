package edu.jhu.cs.damsl.engine.storage.file.factory;

import java.io.FileNotFoundException;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.StorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.SlottedPageFactory;

public class SlottedStorageFileFactory
        implements StorageFileFactory<SlottedPageHeader, SlottedPage, SlottedHeapFile>
{
  protected static final SlottedPageFactory pageFactory
    = new SlottedPageFactory();

  StorageEngine<SlottedPageHeader, SlottedPage, SlottedHeapFile> engine;
  Integer pageSize;
  Long capacity;

  public SlottedStorageFileFactory() {}

  public void initialize(DbEngine<SlottedPageHeader, SlottedPage, SlottedHeapFile> dbms) {
    engine   = dbms.getStorageEngine();
    pageSize = engine.getBufferPool().getPageSize();
    capacity = Defaults.getDefaultFileSize();
  }

  public SlottedPageFactory getPageFactory() {
    return pageFactory;
  }

  public SlottedHeapFile getFile(String fName)
    throws FileNotFoundException
  {
    return getFile(fName, null);
  }

  public SlottedHeapFile getFile(String fName, Schema sch)
    throws FileNotFoundException
  {
    return new SlottedHeapFile(engine, fName, pageSize, capacity, sch);
  }    

  public SlottedHeapFile getFile(FileId id, Schema sch, TableId rel)
    throws FileNotFoundException
  {
    return new SlottedHeapFile(engine, id, sch, rel);
  }
}