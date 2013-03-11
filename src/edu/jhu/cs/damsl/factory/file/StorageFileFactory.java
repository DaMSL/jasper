package edu.jhu.cs.damsl.factory.file;

import java.io.FileNotFoundException;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.FileId.FileKind;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.factory.index.IndexFileFactory;
import edu.jhu.cs.damsl.factory.page.PageFactory;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;

public interface StorageFileFactory<
                    IdType      extends TupleId,
                    HeaderType  extends PageHeader,
                    PageType    extends Page<IdType, HeaderType>,
                    FileType    extends StorageFile<IdType, HeaderType, PageType>>
{

  public void initialize(DbEngine<IdType, HeaderType, PageType, FileType> dbms);

  public Integer getPageSize();

  public PageFactory<IdType, HeaderType, PageType> getPageFactory();

  public TupleIdFactory<IdType> getTupleIdFactory();

  public IndexFileFactory<IdType> getIndexFileFactory();

  public FileType getFile(FileKind k) throws FileNotFoundException;

  public FileType getFile(FileKind k, Schema sch)
    throws FileNotFoundException;

  public FileType getFile(FileId id, Schema sch, TableId rel)
    throws FileNotFoundException;

}