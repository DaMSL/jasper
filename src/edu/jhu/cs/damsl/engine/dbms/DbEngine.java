package edu.jhu.cs.damsl.engine.dbms;

import java.util.HashMap;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.engine.BaseEngine;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.file.factory.StorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public class DbEngine<HeaderType extends PageHeader,
                      PageType extends Page<HeaderType>,
                      FileType extends StorageFile<HeaderType, PageType> >
                extends BaseEngine
{

  StorageEngine<HeaderType, PageType, FileType>  storage;
  
  public DbEngine(StorageFileFactory<HeaderType, PageType, FileType> f)
  {
    storage = new StorageEngine<HeaderType, PageType, FileType> (catalog, f);
    f.initialize(this);
  }

  public DbEngine(String catalogFile,
                  StorageFileFactory<HeaderType, PageType, FileType> f)
  {
    super(catalogFile);
    storage = new StorageEngine<HeaderType, PageType, FileType>(catalog, f);
    f.initialize(this);
  }

  // Accessors
  public StorageEngine<HeaderType, PageType, FileType> 
  getStorageEngine() { return storage; }

  public boolean hasRelation(String tableName) {
    return catalog.getTableNames().contains(tableName);
  }

  public TableId getRelation(String tableName) {
    return catalog.getTableByName(tableName).getId();
  }

  public TableId addRelation(String tableName, Schema schema) {
    TableId tid = catalog.addTable(tableName, schema);
    storage.addRelation(null, tid, schema);
    return tid;
  }

  public String toString() {
    String[] storageLines = storage.toString().split("\\r?\\n");
    String r = "==== Storage Engine ====\n";
    for (String s : storageLines) { r += "  "+s+"\n"; }
    return r;
  }
}
