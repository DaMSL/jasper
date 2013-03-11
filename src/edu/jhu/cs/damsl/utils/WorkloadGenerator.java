package edu.jhu.cs.damsl.utils;

import java.util.LinkedList;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;

public class WorkloadGenerator<IdType     extends TupleId,
                               HeaderType extends PageHeader,
                               PageType   extends Page<IdType,HeaderType>,
                               FileType   extends StorageFile<IdType, HeaderType, PageType>>
{
  protected static final Logger logger = LoggerFactory.getLogger(WorkloadGenerator.class);

  DbEngine<IdType, HeaderType, PageType, FileType> dbms;
  StorageEngine<IdType, HeaderType, PageType, FileType> storage;

  public enum Workload { Sequential, HalfHalf, MostlySequential, MostlyRandom }

  public WorkloadGenerator(DbEngine<IdType, HeaderType, PageType, FileType> _dbms) {
    this.dbms = _dbms;
    this.storage = dbms.getStorageEngine();
  }

  // Issues a block of read requests to the storage engine.
  void request(FileId f, Random random, Workload mode, double p, int max, int offset, int count) {
    boolean sequential =
      (mode == Workload.HalfHalf?           random.nextDouble() >= 0.5 :
        (mode == Workload.MostlySequential? random.nextDouble() <  p :
          (mode == Workload.MostlyRandom?   random.nextDouble() >= p : true)));

    for (int i = 0; i < count; ++i) {
      int pageNum = sequential? ((offset+i) % max) : random.nextInt(max);
      PageId pid = new PageId(f, pageNum);
      storage.getPage(null, pid, Page.Permissions.READ);
    }
  }

  public void generate(TableId t, int requests, int blocksize, Workload mode, double p)
  {
    logger.info(
      "Generating {} requests with block size {} and randomness {}",
      new Object[] { requests, blocksize, p });

    // Get a handle to the first database file supporting the relation.
    LinkedList<FileType> tFiles = storage.getFileManager().getFiles(t);
    if ( tFiles == null || tFiles.isEmpty() ) {
      logger.error("No database files found for relation {}", t.getAddressString());
      return;
    }

    FileType tFile = tFiles.peek();
    FileId fileId = tFile.fileId();
    int numPages = tFile.numPages();

    if ( (mode == Workload.MostlySequential || mode == Workload.MostlyRandom) && p < 0.5 ) { 
      logger.error("Invalid probability for workload, must be greater than 0.5");
      return;
    }

    Random random = new Random();
    int rounds = requests / blocksize;
    int remainder = requests % blocksize;

    // Execute rounds.
    System.out.println("Starting benchmark");
    long startTime = System.nanoTime();
    
    for (int i = 0; i < rounds; ++i) {
      request(fileId, random, mode, p, numPages, i*blocksize, blocksize);
    }
    
    // Execute remainder.
    request(fileId, random, mode, p, numPages, rounds*blocksize, remainder);
    
    long timeSpan = System.nanoTime() - startTime;
    System.out.println("Elapsed: " + Long.toString(timeSpan/1000000) + " ms.");
  }
}
