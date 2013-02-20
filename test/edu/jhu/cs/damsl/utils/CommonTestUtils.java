package edu.jhu.cs.damsl.utils;

import java.util.List;

import edu.jhu.cs.damsl.catalog.Catalog;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.Defaults.SizeUnits;
import edu.jhu.cs.damsl.catalog.Schema.Field;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.file.factory.StorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.language.core.types.DoubleType;
import edu.jhu.cs.damsl.language.core.types.IntType;
import edu.jhu.cs.damsl.language.core.types.LongType;

public class CommonTestUtils<HeaderType extends PageHeader,
                             PageType   extends Page<HeaderType>,
                             FileType   extends StorageFile<HeaderType, PageType>>
{
  protected StorageEngine<HeaderType, PageType, FileType> storage;
  protected DbBufferPool<HeaderType, PageType, FileType> pool;
  protected StandardGenerator dataGen;

  public CommonTestUtils(StorageFileFactory<HeaderType, PageType, FileType> f) {
    Catalog c = new Catalog();
    
    storage = new StorageEngine<HeaderType, PageType, FileType>(c, f);    
    
    pool = new DbBufferPool<HeaderType, PageType, FileType>(
                storage, 20, SizeUnits.Mega, 4, SizeUnits.Kilo);
    
    dataGen = new StandardGenerator();
  }

  public StorageEngine<HeaderType, PageType, FileType>
  getStorage() { return storage; }

  public DbBufferPool<HeaderType, PageType, FileType>
  getPool() { return pool; }

  public StandardGenerator getDataGenerator() { return dataGen; }

  // Schema helpers.
  
  public static Schema getLIDSchema() {
    Field xl = new Schema("test").new Field("x", new LongType());
    Field yi = new Schema("test").new Field("y", new IntType());
    Field zd = new Schema("test").new Field("z", new DoubleType());
    return new Schema("lid", xl, yi, zd);
  }

  public static Schema getIIISchema() {
    Field xi = new Schema("test").new Field("x", new IntType());
    Field yi = new Schema("test").new Field("y", new IntType());
    Field zi = new Schema("test").new Field("z", new IntType());
    return new Schema("iii", xi, yi, zi);
  }
  
  // Tuple construction
  
  public List<Tuple> getTuples() { return dataGen.generateData(); }
  
  public List<Tuple> getTuples(Schema sch, int numTuples) {
    return dataGen.generateData(sch, numTuples);
  }
  
}
