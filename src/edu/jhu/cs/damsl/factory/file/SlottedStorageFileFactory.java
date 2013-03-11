package edu.jhu.cs.damsl.factory.file;

import java.io.FileNotFoundException;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.FileId.FileKind;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.SlottedTupleId;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.factory.page.SlottedPageFactory;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;

public class SlottedStorageFileFactory
        extends BaseStorageFileFactory<SlottedTupleId, SlottedPageHeader,
                                       SlottedPage, SlottedHeapFile>
{
  protected static final SlottedPageFactory pageFactory
    = new SlottedPageFactory();

  public SlottedStorageFileFactory() {}

  @Override
  public SlottedPageFactory getPageFactory() {
    return pageFactory;
  }

  @Override
  public TupleIdFactory<SlottedTupleId> getTupleIdFactory() {
    return SlottedPage.tupleIdFactory;
  }

  @Override
  public SlottedHeapFile getFile(FileKind k)
    throws FileNotFoundException
  {
    return getFile(k, null);
  }

  @Override
  public SlottedHeapFile getFile(FileKind k, Schema sch)
    throws FileNotFoundException
  {
    return new SlottedHeapFile(engine, k, pageSize, capacity, sch);
  }    

  @Override
  public SlottedHeapFile getFile(FileId id, Schema sch, TableId rel)
    throws FileNotFoundException
  {
    return new SlottedHeapFile(engine, id, sch, rel);
  }
}