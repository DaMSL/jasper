package edu.jhu.cs.damsl.engine;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Catalog;
import edu.jhu.cs.damsl.catalog.Defaults;

/*
 * An abstract engine superclass, providing a catalog and network manager
 * for all data management systems.
 */
public abstract class BaseEngine {
  protected static final Logger logger = LoggerFactory.getLogger(BaseEngine.class);

  protected Catalog catalog;
  
  public BaseEngine() {
    logger.info("Initializing {}", Defaults.systemName);
    catalog = new Catalog();
  }

  // Engine initialization from a saved catalog.
  public BaseEngine(String catalogFile) {
    logger.info("Initializing {}", Defaults.systemName); 
    try {
      FileInputStream f = new FileInputStream(catalogFile);
      ObjectInputStream o = new ObjectInputStream(f);
      catalog = (Catalog) o.readObject();
      o.close();
    } catch (Exception e) { e.printStackTrace(); }
  }

  public void saveDatabase(String catalogFile) {
    try {
      FileOutputStream f = new FileOutputStream(catalogFile);
      ObjectOutputStream o = new ObjectOutputStream(f);
      o.writeObject(catalog);
      o.flush();
      o.close();
    } catch (Exception e) { e.printStackTrace(); }
  }

  /**
   * @return the DBMS catalog.
   */
  public Catalog getCatalog() { return catalog; }

}
