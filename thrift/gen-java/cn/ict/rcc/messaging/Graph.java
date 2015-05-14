/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package cn.ict.rcc.messaging;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Graph implements org.apache.thrift.TBase<Graph, Graph._Fields>, java.io.Serializable, Cloneable, Comparable<Graph> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Graph");

  private static final org.apache.thrift.protocol.TField VERTEXES_FIELD_DESC = new org.apache.thrift.protocol.TField("vertexes", org.apache.thrift.protocol.TType.MAP, (short)1);
  private static final org.apache.thrift.protocol.TField SERVERS_INVOLVED_FIELD_DESC = new org.apache.thrift.protocol.TField("serversInvolved", org.apache.thrift.protocol.TType.MAP, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new GraphStandardSchemeFactory());
    schemes.put(TupleScheme.class, new GraphTupleSchemeFactory());
  }

  public Map<String,String> vertexes; // required
  public Map<String,Set<String>> serversInvolved; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    VERTEXES((short)1, "vertexes"),
    SERVERS_INVOLVED((short)2, "serversInvolved");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // VERTEXES
          return VERTEXES;
        case 2: // SERVERS_INVOLVED
          return SERVERS_INVOLVED;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private _Fields optionals[] = {_Fields.SERVERS_INVOLVED};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.VERTEXES, new org.apache.thrift.meta_data.FieldMetaData("vertexes", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.SERVERS_INVOLVED, new org.apache.thrift.meta_data.FieldMetaData("serversInvolved", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.SetMetaData(org.apache.thrift.protocol.TType.SET, 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Graph.class, metaDataMap);
  }

  public Graph() {
  }

  public Graph(
    Map<String,String> vertexes)
  {
    this();
    this.vertexes = vertexes;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Graph(Graph other) {
    if (other.isSetVertexes()) {
      Map<String,String> __this__vertexes = new HashMap<String,String>(other.vertexes);
      this.vertexes = __this__vertexes;
    }
    if (other.isSetServersInvolved()) {
      Map<String,Set<String>> __this__serversInvolved = new HashMap<String,Set<String>>(other.serversInvolved.size());
      for (Map.Entry<String, Set<String>> other_element : other.serversInvolved.entrySet()) {

        String other_element_key = other_element.getKey();
        Set<String> other_element_value = other_element.getValue();

        String __this__serversInvolved_copy_key = other_element_key;

        Set<String> __this__serversInvolved_copy_value = new HashSet<String>(other_element_value);

        __this__serversInvolved.put(__this__serversInvolved_copy_key, __this__serversInvolved_copy_value);
      }
      this.serversInvolved = __this__serversInvolved;
    }
  }

  public Graph deepCopy() {
    return new Graph(this);
  }

  @Override
  public void clear() {
    this.vertexes = null;
    this.serversInvolved = null;
  }

  public int getVertexesSize() {
    return (this.vertexes == null) ? 0 : this.vertexes.size();
  }

  public void putToVertexes(String key, String val) {
    if (this.vertexes == null) {
      this.vertexes = new HashMap<String,String>();
    }
    this.vertexes.put(key, val);
  }

  public Map<String,String> getVertexes() {
    return this.vertexes;
  }

  public Graph setVertexes(Map<String,String> vertexes) {
    this.vertexes = vertexes;
    return this;
  }

  public void unsetVertexes() {
    this.vertexes = null;
  }

  /** Returns true if field vertexes is set (has been assigned a value) and false otherwise */
  public boolean isSetVertexes() {
    return this.vertexes != null;
  }

  public void setVertexesIsSet(boolean value) {
    if (!value) {
      this.vertexes = null;
    }
  }

  public int getServersInvolvedSize() {
    return (this.serversInvolved == null) ? 0 : this.serversInvolved.size();
  }

  public void putToServersInvolved(String key, Set<String> val) {
    if (this.serversInvolved == null) {
      this.serversInvolved = new HashMap<String,Set<String>>();
    }
    this.serversInvolved.put(key, val);
  }

  public Map<String,Set<String>> getServersInvolved() {
    return this.serversInvolved;
  }

  public Graph setServersInvolved(Map<String,Set<String>> serversInvolved) {
    this.serversInvolved = serversInvolved;
    return this;
  }

  public void unsetServersInvolved() {
    this.serversInvolved = null;
  }

  /** Returns true if field serversInvolved is set (has been assigned a value) and false otherwise */
  public boolean isSetServersInvolved() {
    return this.serversInvolved != null;
  }

  public void setServersInvolvedIsSet(boolean value) {
    if (!value) {
      this.serversInvolved = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case VERTEXES:
      if (value == null) {
        unsetVertexes();
      } else {
        setVertexes((Map<String,String>)value);
      }
      break;

    case SERVERS_INVOLVED:
      if (value == null) {
        unsetServersInvolved();
      } else {
        setServersInvolved((Map<String,Set<String>>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case VERTEXES:
      return getVertexes();

    case SERVERS_INVOLVED:
      return getServersInvolved();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case VERTEXES:
      return isSetVertexes();
    case SERVERS_INVOLVED:
      return isSetServersInvolved();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Graph)
      return this.equals((Graph)that);
    return false;
  }

  public boolean equals(Graph that) {
    if (that == null)
      return false;

    boolean this_present_vertexes = true && this.isSetVertexes();
    boolean that_present_vertexes = true && that.isSetVertexes();
    if (this_present_vertexes || that_present_vertexes) {
      if (!(this_present_vertexes && that_present_vertexes))
        return false;
      if (!this.vertexes.equals(that.vertexes))
        return false;
    }

    boolean this_present_serversInvolved = true && this.isSetServersInvolved();
    boolean that_present_serversInvolved = true && that.isSetServersInvolved();
    if (this_present_serversInvolved || that_present_serversInvolved) {
      if (!(this_present_serversInvolved && that_present_serversInvolved))
        return false;
      if (!this.serversInvolved.equals(that.serversInvolved))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(Graph other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetVertexes()).compareTo(other.isSetVertexes());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetVertexes()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.vertexes, other.vertexes);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetServersInvolved()).compareTo(other.isSetServersInvolved());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetServersInvolved()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.serversInvolved, other.serversInvolved);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Graph(");
    boolean first = true;

    sb.append("vertexes:");
    if (this.vertexes == null) {
      sb.append("null");
    } else {
      sb.append(this.vertexes);
    }
    first = false;
    if (isSetServersInvolved()) {
      if (!first) sb.append(", ");
      sb.append("serversInvolved:");
      if (this.serversInvolved == null) {
        sb.append("null");
      } else {
        sb.append(this.serversInvolved);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (vertexes == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'vertexes' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class GraphStandardSchemeFactory implements SchemeFactory {
    public GraphStandardScheme getScheme() {
      return new GraphStandardScheme();
    }
  }

  private static class GraphStandardScheme extends StandardScheme<Graph> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Graph struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // VERTEXES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map24 = iprot.readMapBegin();
                struct.vertexes = new HashMap<String,String>(2*_map24.size);
                for (int _i25 = 0; _i25 < _map24.size; ++_i25)
                {
                  String _key26;
                  String _val27;
                  _key26 = iprot.readString();
                  _val27 = iprot.readString();
                  struct.vertexes.put(_key26, _val27);
                }
                iprot.readMapEnd();
              }
              struct.setVertexesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // SERVERS_INVOLVED
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map28 = iprot.readMapBegin();
                struct.serversInvolved = new HashMap<String,Set<String>>(2*_map28.size);
                for (int _i29 = 0; _i29 < _map28.size; ++_i29)
                {
                  String _key30;
                  Set<String> _val31;
                  _key30 = iprot.readString();
                  {
                    org.apache.thrift.protocol.TSet _set32 = iprot.readSetBegin();
                    _val31 = new HashSet<String>(2*_set32.size);
                    for (int _i33 = 0; _i33 < _set32.size; ++_i33)
                    {
                      String _elem34;
                      _elem34 = iprot.readString();
                      _val31.add(_elem34);
                    }
                    iprot.readSetEnd();
                  }
                  struct.serversInvolved.put(_key30, _val31);
                }
                iprot.readMapEnd();
              }
              struct.setServersInvolvedIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, Graph struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.vertexes != null) {
        oprot.writeFieldBegin(VERTEXES_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.vertexes.size()));
          for (Map.Entry<String, String> _iter35 : struct.vertexes.entrySet())
          {
            oprot.writeString(_iter35.getKey());
            oprot.writeString(_iter35.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.serversInvolved != null) {
        if (struct.isSetServersInvolved()) {
          oprot.writeFieldBegin(SERVERS_INVOLVED_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.SET, struct.serversInvolved.size()));
            for (Map.Entry<String, Set<String>> _iter36 : struct.serversInvolved.entrySet())
            {
              oprot.writeString(_iter36.getKey());
              {
                oprot.writeSetBegin(new org.apache.thrift.protocol.TSet(org.apache.thrift.protocol.TType.STRING, _iter36.getValue().size()));
                for (String _iter37 : _iter36.getValue())
                {
                  oprot.writeString(_iter37);
                }
                oprot.writeSetEnd();
              }
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class GraphTupleSchemeFactory implements SchemeFactory {
    public GraphTupleScheme getScheme() {
      return new GraphTupleScheme();
    }
  }

  private static class GraphTupleScheme extends TupleScheme<Graph> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Graph struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.vertexes.size());
        for (Map.Entry<String, String> _iter38 : struct.vertexes.entrySet())
        {
          oprot.writeString(_iter38.getKey());
          oprot.writeString(_iter38.getValue());
        }
      }
      BitSet optionals = new BitSet();
      if (struct.isSetServersInvolved()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetServersInvolved()) {
        {
          oprot.writeI32(struct.serversInvolved.size());
          for (Map.Entry<String, Set<String>> _iter39 : struct.serversInvolved.entrySet())
          {
            oprot.writeString(_iter39.getKey());
            {
              oprot.writeI32(_iter39.getValue().size());
              for (String _iter40 : _iter39.getValue())
              {
                oprot.writeString(_iter40);
              }
            }
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Graph struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TMap _map41 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
        struct.vertexes = new HashMap<String,String>(2*_map41.size);
        for (int _i42 = 0; _i42 < _map41.size; ++_i42)
        {
          String _key43;
          String _val44;
          _key43 = iprot.readString();
          _val44 = iprot.readString();
          struct.vertexes.put(_key43, _val44);
        }
      }
      struct.setVertexesIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TMap _map45 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.SET, iprot.readI32());
          struct.serversInvolved = new HashMap<String,Set<String>>(2*_map45.size);
          for (int _i46 = 0; _i46 < _map45.size; ++_i46)
          {
            String _key47;
            Set<String> _val48;
            _key47 = iprot.readString();
            {
              org.apache.thrift.protocol.TSet _set49 = new org.apache.thrift.protocol.TSet(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
              _val48 = new HashSet<String>(2*_set49.size);
              for (int _i50 = 0; _i50 < _set49.size; ++_i50)
              {
                String _elem51;
                _elem51 = iprot.readString();
                _val48.add(_elem51);
              }
            }
            struct.serversInvolved.put(_key47, _val48);
          }
        }
        struct.setServersInvolvedIsSet(true);
      }
    }
  }

}
