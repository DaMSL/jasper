package edu.jhu.cs.damsl.engine.storage.file.factory;

import java.io.FileNotFoundException;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.file.ContiguousHeapFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.StorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.ContiguousPageFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class ContiguousStorageFileFactory
        implements StorageFileFactory<PageHeader, ContiguousPage, ContiguousHeapFile>
{
  public ContiguousStorageFileFactory() {}

  public void initialize(DbEngine<PageHeader, ContiguousPage, ContiguousHeapFile> dbms) {}

  public ContiguousPageFactory getPageFactory() { return null; }

  public ContiguousHeapFile getFile(String fName)
    throws FileNotFoundException
  {
    return null;
  }

  public ContiguousHeapFile getFile(String fName, Schema sch)
    throws FileNotFoundException
  {
    return null;
  }    

  public ContiguousHeapFile getFile(FileId id, Schema sch, TableId rel)
    throws FileNotFoundException
  {
    return null;
  }

}
