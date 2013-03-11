package edu.jhu.cs.damsl.factory.file;

import java.io.FileNotFoundException;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.FileId.FileKind;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.file.ContiguousHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.factory.file.StorageFileFactory;
import edu.jhu.cs.damsl.factory.page.ContiguousPageFactory;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;

public class ContiguousStorageFileFactory
        extends BaseStorageFileFactory<ContiguousTupleId, PageHeader,
                                       ContiguousPage, ContiguousHeapFile>
{
  protected static final ContiguousPageFactory pageFactory
    = new ContiguousPageFactory();

  public ContiguousStorageFileFactory() {}

  @Override
  public ContiguousPageFactory getPageFactory() { return pageFactory; }

  @Override
  public TupleIdFactory<ContiguousTupleId> getTupleIdFactory() {
    return ContiguousPage.tupleIdFactory;
  }

  @Override
  public ContiguousHeapFile getFile(FileKind k)
    throws FileNotFoundException
  {
    return getFile(k, null);
  }

  @Override
  public ContiguousHeapFile getFile(FileKind k, Schema sch)
    throws FileNotFoundException
  {
    return new ContiguousHeapFile(engine, k, pageSize, capacity, sch);
  }    

  @Override
  public ContiguousHeapFile getFile(FileId id, Schema sch, TableId rel)
    throws FileNotFoundException
  {
    return new ContiguousHeapFile(engine, id, sch, rel);
  }

}
