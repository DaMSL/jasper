package edu.jhu.cs.damsl.factory.index;

import java.io.FileNotFoundException;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.FileId.FileKind;
import edu.jhu.cs.damsl.catalog.identifiers.IndexId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.index.IndexFile;
import edu.jhu.cs.damsl.engine.storage.index.IndexPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;

public interface IndexFileFactory<IdType extends TupleId> {
  public IndexFile<IdType> 
  getIndexFile(DbBufferPool<IndexId, ContiguousTupleId, PageHeader,
                            IndexPage<IdType>, IndexFile<IdType>> pool,
               TupleIdFactory<IdType> factory,
               IndexId id, Schema key, FileKind kind)
    throws FileNotFoundException;
}