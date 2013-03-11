package edu.jhu.cs.damsl.catalog.identifiers;

import edu.jhu.cs.damsl.catalog.Addressable;
import edu.jhu.cs.damsl.catalog.Schema;

public class IndexId implements Addressable {
  String id;
  TableId rel;
  Schema key;

  public IndexId(TableId r, Schema k) {
    rel = r;
    key = k;
    String fieldNames = "";
    for (String f : k.getFields())
      fieldNames += (fieldNames.isEmpty()? "" : ",") + f;
    id = "IDX"+r.getAddressString()+"("+fieldNames+")";
  }

  @Override
  public boolean equals(Object o) {
    if ( o == null || !(o instanceof IndexId) ) { return false; }
    IndexId other = (IndexId) o;
    return o == this || (rel.equals(other.rel) && key.namedMatch(other.key));
  }

  @Override
  public int hashCode() { return rel.hashCode() + key.hashCode(); }

  public TableId relation() { return rel; }

  public Schema schema() { return key; }

  @Override
  public int getAddress() { return getAddressString().hashCode(); }

  @Override
  public String getAddressString() { return id; }

}
