package edu.jhu.cs.damsl.catalog.identifiers;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import edu.jhu.cs.damsl.catalog.Addressable;
import edu.jhu.cs.damsl.catalog.Durable;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;

public class TableId implements Durable, Serializable {
  private static Integer counter = 0;
  protected String relName;
  protected Integer relId;
  protected LinkedList<FileId> relFiles;
  
  public TableId(String name) {
    relName = name;
    relId = counter++;
    relFiles = new LinkedList<FileId>();
  }

  public String name() { return relName; }
  public List<FileId> files() { return relFiles; }

  @Override
  public boolean equals(Object o) {
    if ( o == null || !(o instanceof TableId) ) { return false; }
    return o == this || (((TableId) o).relId == relId);
  }

  @Override
  public int hashCode() { return relId; }

  @Override
  public int getAddress() { return hashCode(); }

  @Override
  public String getAddressString() { return relName+"("+relId+")"; }

}
