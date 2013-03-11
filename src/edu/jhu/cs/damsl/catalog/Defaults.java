package edu.jhu.cs.damsl.catalog;

import java.nio.charset.Charset;


public class Defaults {
  public static String versionNumber = "0.1";
  public static String systemName = "Jasper v"+versionNumber;

  // String helpers.

  // Default charset to use for string types.
  public static Charset defaultCharset = Charset.forName("UTF-16");

  // Schema and typing parameters.

  // Default name of a field in a schema
  public static String defaultColumnName = "out";
  
  // Operator parameters.

  // Default number of tuples to process on one invocation of an operator.
  public static Integer defaultOperatorBatchSize = 100;
  
  // Max number of output streams per operator.
  public static Integer maxOperatorOutDegree = 64;

  // Statistics parameters.

  // Number of samples to maintain for per statistics item.
  public static Integer defaultStatsWindowSize = 10;

  // Core engine parameters.
  public enum SizeUnits { Kilo, Mega, Giga };

  // Database files.
  public static String defaultDbFilePrefix = "jdbf";

  // Default buffer pool size
  public static Integer defaultBufferPoolSize = 20;
  public static SizeUnits defaultBufferPoolUnit = SizeUnits.Mega;

  // Default index page pool size
  public static Integer defaultIndexBufferPoolSize = 20;
  public static SizeUnits defaultIndexBufferPoolUnit = SizeUnits.Mega;
  
  // Default relation-page tracking cache capacity
  public static Integer defaultPoolContentCacheSize = 10000;

  // Default page size
  public static Integer defaultPageSize = 4;
  public static SizeUnits defaultPageUnit = SizeUnits.Kilo;

  // Default number of tuple slots per page.
  public static Integer defaultPageSlots = 1024;
  
  // Default storage file size.
  public static Integer defaultFileSize = 20;
  public static SizeUnits defaultFileSizeUnit = SizeUnits.Mega;
  
  public static Long getDefaultFileSize() {
    return Defaults.getSizeAsLong(
        Defaults.defaultFileSize, Defaults.defaultFileSizeUnit);
  }

  // Default queue size in pages.
  public static Integer defaultQueueSize = 10;
  
  // Default # of worker threads.
  public static Integer defaultNumWorkers = 1;
  
  // Default weight for schedulable if it has not been profiled.
  public static Double defaultSchedulingPriority = 1.0;

  // Index parameters.
  public static Double defaultFillFactor = 0.5;

  // Policy parameters.

  // Local buffer reclamation frequency in terms of # of dequeues
  public static Integer defaultLocalReclamationPeriod = 100;

  // System-wide reclamation frequency in terms of # of operator invocations
  public static Integer defaultGlobalReclamationPeriod = 100;

  // Fraction of free space remaining in queue before reclamation
  public static Double defaultQueueReclamationFraction = 0.2;
  
  // Fraction of JVM memory used before global queue reclamation
  public static Double defaultGlobalUsageFraction = 0.8;
  
  // Network parameters.
  public static Integer defaultNetworkThreads = 20;
  
  public static Integer defaultPortOffset = 8000;

  // Size computation helpers.
  public static Integer getSizeAsInteger(Integer size, SizeUnits unit) {
    Integer r = size;
    switch (unit) {
    case Kilo: r <<= 10; break;
    case Mega: r <<= 20; break;
    case Giga: r <<= 30; break;
    }
    return r;
  }

  public static Long getSizeAsLong(Integer size, SizeUnits unit) {
    Long r = size.longValue();
    switch (unit) {
    case Kilo: r <<= 10; break;
    case Mega: r <<= 20; break;
    case Giga: r <<= 30; break;
    }
    return r;
  }
}
