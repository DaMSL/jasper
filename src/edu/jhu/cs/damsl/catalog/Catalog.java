package edu.jhu.cs.damsl.catalog;

import java.io.Serializable;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.IndexId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.specs.TableSpec;

import edu.jhu.cs.damsl.language.core.types.TypeException;
import edu.jhu.cs.damsl.language.core.types.Typing;

public class Catalog implements Serializable {

  public class CatalogException extends Exception {
    private static final long serialVersionUID = -5397774116788913532L;
    public CatalogException() {}
    public CatalogException(String msg) { super(msg); }
  }
  
  protected static final Logger logger = LoggerFactory.getLogger(Catalog.class);

  // Base relations.
  HashMap<String, TableSpec> relations;

  // Indexes over base relations.
  HashMap<TableId, LinkedList<IndexId>> indexes;

  public Catalog() {
    relations = new HashMap<String, TableSpec>();
    indexes = new HashMap<TableId, LinkedList<IndexId>>();
  }

  // Return a shadow catalog. This is very similar to a clone operation,
  // except that we do not perform a deep copy of catalog elements, only the
  // catalog container data structures.
  @SuppressWarnings("unchecked")
  public Catalog getShadow() {
    Catalog cloned = null;
    try {
      cloned = (Catalog) super.clone();
    } catch (CloneNotSupportedException e) { e.printStackTrace(); }

    if ( cloned != null ) {
      // No need to deep clone collection objects,
      cloned.relations = (HashMap<String, TableSpec>) relations.clone();
    }
    return cloned;
  }

  // Table accessors
  public TableId addTable(String tableName, Schema schema) {
    TableSpec ts = new TableSpec(tableName, schema);
    relations.put(tableName, ts);
    indexes.put(ts.getId(), new LinkedList<IndexId>());
    return ts.getId();
  }
  
  public void removeTable(TableSpec t) { relations.remove(t.getName()); }

  public Map<String, TableSpec> getTables() { return relations; }

  public Set<String> getTableNames() { return relations.keySet(); }

  public TableSpec getTableByName(String tableName) {
      return this.relations.get(tableName);
  }

  public void mergeTables(Catalog c) { 
    for (Map.Entry<String, TableSpec> e : c.relations.entrySet()) {
      if ( !relations.containsKey(e.getKey()) ) {
        try {
          addTable(e.getKey(), e.getValue().getSchema());
        } catch (TypeException t) {
          logger.error("could not merge table {}, no schema found.", e.getKey());
        }
      }
    }
  }

  // Index accessors.

  // Adds an index with the given key schema to the relation.
  // TODO: typecheck index key to ensure its attributes are present in the relation.
  public IndexId addIndex(TableId rel, Schema keySchema) {
    IndexId r = null;
    boolean found = false;
    LinkedList<IndexId> existingIndexes = null;
    if ( indexes.containsKey(rel) ) {
      existingIndexes = indexes.get(rel);
      for ( IndexId id : existingIndexes ) {
        if ( id.schema().namedMatch(keySchema) ) { found = true; break; }
      }
    } else {
      existingIndexes = new LinkedList<IndexId>();
      indexes.put(rel, existingIndexes);
    }

    // Add index only if it does not exist alredy.
    if ( !found ) {
      r = new IndexId(rel, keySchema);
      existingIndexes.add(r);
    }

    return r;
  }

  // Removes the index corresponding to the given index id.
  public void removeIndex(IndexId id)
  {
    TableId rel = id.relation();
    LinkedList<IndexId> existingIndexes = indexes.get(rel);
    if ( existingIndexes != null ) { existingIndexes.remove(id); }
  }

  // Retrieves all indexes availale for the given relation.
  public LinkedList<IndexId> getIndexes(TableId rel)  {
    return indexes.get(rel);
  }

}
