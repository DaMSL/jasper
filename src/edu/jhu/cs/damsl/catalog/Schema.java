package edu.jhu.cs.damsl.catalog;

import java.io.Serializable;
import java.util.*;

import edu.jhu.cs.damsl.language.core.types.Type;

public class Schema implements Serializable {
  
  public class SchemaException extends Throwable {
    private static final long serialVersionUID = 3892626674614780691L;
  }
  
  // Helper class for direct construction.
  public class Field {
    String name;
    Type type;
    public Field(String n, Type t) { name = n; type = t; }
  }

  String name;
  
  // Currently this is optimized for retrieving types, rather than field
  // positions. In the latter case, we should switch to LinkedList or store
  // positions explicitly (they're unlikely to change).
  LinkedHashMap<String, Type> fieldsAndTypes;
  
  public Schema(String name) {
    this.name = name;
  }
  
  public Schema(String name, Map<String,Type> schema) {
    this(name);
    fieldsAndTypes = new LinkedHashMap<String, Type>(schema);
  }
  
  public Schema(String name, Field... fields) {
    this(name);
    fieldsAndTypes = new LinkedHashMap<String, Type>();
    for ( Field f : fields ) {
      fieldsAndTypes.put(f.name, f.type);
    }
  }

  public String getName() { return name; }

  // Accessors that preserve schema order.
  public List<String> getFields() { 
    LinkedList<String> r = new LinkedList<String>();
    for (Map.Entry<String, Type> e : fieldsAndTypes.entrySet())
      r.add(e.getKey());
    return r; 
  }

  public List<Type> getTypes() { 
    LinkedList<Type> r = new LinkedList<Type>();
    for (Map.Entry<String, Type> e : fieldsAndTypes.entrySet()) 
      r.add(e.getValue());
    return r;
  }
  
  public Map<String, Type> getFieldsAndTypes() { return fieldsAndTypes; }

  public boolean hasField(String fieldName) {
    return fieldsAndTypes.containsKey(fieldName);
  }

  public Integer getFieldPosition(String fieldName) {
    Integer r = null;
    Integer x = 0;
    for (Map.Entry<String, Type> e : fieldsAndTypes.entrySet()) {
      if ( e.getKey().equals(fieldName) ) { r = x; break; }
      ++x;
    }
    return r;
  }
  
  public Type getFieldType(String fieldName) throws SchemaException {
    Type r = fieldsAndTypes.get(fieldName);
    if (r == null) throw new SchemaException();
    return r;
  }

  // Returns tuple size if all fields are fixed length, otherwise -1.
  public Integer getTupleSize() {
    Integer r = 0;
    for (Type t : fieldsAndTypes.values()) {
      if (t.getSize() > 0) r += t.getSize();
      else { r = t.getSize(); break; }
    }
    return r;
  }

  // Returns the size of this schema with fields taken as the given objects.
  // This can be used to compute the size of a variable length tuple filled
  // with the given fields prior to tuple allocation.
  // Returns -1 if there is a mismatch on the number of fields.
  public Integer getTupleSize(List<Object> fields) {
    Integer r = 0;
    if ( fieldsAndTypes.size() != fields.size() ) return -1;
    ListIterator<Object> it = fields.listIterator();
    for (Type t : fieldsAndTypes.values()) {
      if ( it.hasNext() ) {
        Integer fSize = t.getInstanceSize(it.next());
        r = (fSize > 0? r+fSize : fSize);
      } else { r = -1; }
      if ( r < 0 ) break;
    }
    return r;
  }
  
  // Schema operations

  // Concatenation, returns a new schema concatenating this one, and the given.
  public Schema concat(String name, Schema other) {
    Schema r = new Schema(name, fieldsAndTypes);
    r.fieldsAndTypes.putAll(other.fieldsAndTypes);
    return r;
  }

  // Named matching: two schemas must have the same names, types and ordering.
  public boolean namedMatch(Schema other) {
    return other == null? false : fieldsAndTypes.equals(other.fieldsAndTypes);
  }
  
  // Unnamed matching: two schemas must have same types and ordering
  public boolean unnamedMatch(Schema other) {
    return other == null? false : getTypes().equals(other.getTypes());
  }
  
  @Override
  public String toString() {
    String fields = "";
    for (Map.Entry<String, Type> e : fieldsAndTypes.entrySet()) { 
      fields += (fields.isEmpty()? "" : ",") +
        e.getKey() + ":" + e.getValue().getTypeName();
    }
    return name + "<"+fields+">["+getTupleSize()+"]";
  }
}
