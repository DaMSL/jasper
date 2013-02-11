package edu.jhu.cs.damsl.engine.storage.file.factory;

import java.io.FileNotFoundException;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageFactory;

public interface StorageFileFactory<
                    HeaderType extends PageHeader,
                    PageType extends Page<HeaderType>,
                    FileType extends StorageFile<HeaderType, PageType>>
{

  public void initialize(DbEngine<HeaderType, PageType, FileType> dbms);

  public PageFactory<HeaderType, PageType> getPageFactory();

  public FileType getFile(String fName) throws FileNotFoundException;

  public FileType getFile(String fName, Schema sch)
    throws FileNotFoundException;

  public FileType getFile(FileId id, Schema sch, TableId rel)
    throws FileNotFoundException;

}