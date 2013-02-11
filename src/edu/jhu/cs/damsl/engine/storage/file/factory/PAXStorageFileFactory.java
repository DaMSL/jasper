package edu.jhu.cs.damsl.engine.storage.file.factory;

import java.io.FileNotFoundException;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.file.PAXHeapFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.StorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.page.PAXPage;
import edu.jhu.cs.damsl.engine.storage.page.PAXPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.PAXPageFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class PAXStorageFileFactory
        implements StorageFileFactory<PAXPageHeader, PAXPage, PAXHeapFile>
{

  public PAXStorageFileFactory() {}

  public void initialize(DbEngine<PAXPageHeader, PAXPage, PAXHeapFile> dbms) {}

  public PAXPageFactory getPageFactory() { return null; }

  public PAXHeapFile getFile(String fName)
    throws FileNotFoundException
  {
    return null;
  }

  public PAXHeapFile getFile(String fName, Schema sch)
    throws FileNotFoundException
  {
    return null;
  }    

  public PAXHeapFile getFile(FileId id, Schema sch, TableId rel)
    throws FileNotFoundException
  {
    return null;
  }

}
