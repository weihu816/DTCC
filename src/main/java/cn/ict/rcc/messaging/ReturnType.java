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

public class ReturnType implements org.apache.thrift.TBase<ReturnType, ReturnType._Fields>, java.io.Serializable, Cloneable, Comparable<ReturnType> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ReturnType");

  private static final org.apache.thrift.protocol.TField OUTPUT_FIELD_DESC = new org.apache.thrift.protocol.TField("output", org.apache.thrift.protocol.TType.MAP, (short)1);
  private static final org.apache.thrift.protocol.TField DEP_FIELD_DESC = new org.apache.thrift.protocol.TField("dep", org.apache.thrift.protocol.TType.STRUCT, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ReturnTypeStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ReturnTypeTupleSchemeFactory());
  }

  public Map<String,String> output; // required
  public Graph dep; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    OUTPUT((short)1, "output"),
    DEP((short)2, "dep");

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
        case 1: // OUTPUT
          return OUTPUT;
        case 2: // DEP
          return DEP;
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
  private _Fields optionals[] = {_Fields.DEP};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.OUTPUT, new org.apache.thrift.meta_data.FieldMetaData("output", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.DEP, new org.apache.thrift.meta_data.FieldMetaData("dep", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Graph.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ReturnType.class, metaDataMap);
  }

  public ReturnType() {
  }

  public ReturnType(
    Map<String,String> output)
  {
    this();
    this.output = output;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ReturnType(ReturnType other) {
    if (other.isSetOutput()) {
      Map<String,String> __this__output = new HashMap<String,String>(other.output);
      this.output = __this__output;
    }
    if (other.isSetDep()) {
      this.dep = new Graph(other.dep);
    }
  }

  public ReturnType deepCopy() {
    return new ReturnType(this);
  }

  @Override
  public void clear() {
    this.output = null;
    this.dep = null;
  }

  public int getOutputSize() {
    return (this.output == null) ? 0 : this.output.size();
  }

  public void putToOutput(String key, String val) {
    if (this.output == null) {
      this.output = new HashMap<String,String>();
    }
    this.output.put(key, val);
  }

  public Map<String,String> getOutput() {
    return this.output;
  }

  public ReturnType setOutput(Map<String,String> output) {
    this.output = output;
    return this;
  }

  public void unsetOutput() {
    this.output = null;
  }

  /** Returns true if field output is set (has been assigned a value) and false otherwise */
  public boolean isSetOutput() {
    return this.output != null;
  }

  public void setOutputIsSet(boolean value) {
    if (!value) {
      this.output = null;
    }
  }

  public Graph getDep() {
    return this.dep;
  }

  public ReturnType setDep(Graph dep) {
    this.dep = dep;
    return this;
  }

  public void unsetDep() {
    this.dep = null;
  }

  /** Returns true if field dep is set (has been assigned a value) and false otherwise */
  public boolean isSetDep() {
    return this.dep != null;
  }

  public void setDepIsSet(boolean value) {
    if (!value) {
      this.dep = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case OUTPUT:
      if (value == null) {
        unsetOutput();
      } else {
        setOutput((Map<String,String>)value);
      }
      break;

    case DEP:
      if (value == null) {
        unsetDep();
      } else {
        setDep((Graph)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case OUTPUT:
      return getOutput();

    case DEP:
      return getDep();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case OUTPUT:
      return isSetOutput();
    case DEP:
      return isSetDep();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ReturnType)
      return this.equals((ReturnType)that);
    return false;
  }

  public boolean equals(ReturnType that) {
    if (that == null)
      return false;

    boolean this_present_output = true && this.isSetOutput();
    boolean that_present_output = true && that.isSetOutput();
    if (this_present_output || that_present_output) {
      if (!(this_present_output && that_present_output))
        return false;
      if (!this.output.equals(that.output))
        return false;
    }

    boolean this_present_dep = true && this.isSetDep();
    boolean that_present_dep = true && that.isSetDep();
    if (this_present_dep || that_present_dep) {
      if (!(this_present_dep && that_present_dep))
        return false;
      if (!this.dep.equals(that.dep))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(ReturnType other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetOutput()).compareTo(other.isSetOutput());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOutput()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.output, other.output);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDep()).compareTo(other.isSetDep());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDep()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dep, other.dep);
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
    StringBuilder sb = new StringBuilder("ReturnType(");
    boolean first = true;

    sb.append("output:");
    if (this.output == null) {
      sb.append("null");
    } else {
      sb.append(this.output);
    }
    first = false;
    if (isSetDep()) {
      if (!first) sb.append(", ");
      sb.append("dep:");
      if (this.dep == null) {
        sb.append("null");
      } else {
        sb.append(this.dep);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (output == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'output' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
    if (dep != null) {
      dep.validate();
    }
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

  private static class ReturnTypeStandardSchemeFactory implements SchemeFactory {
    public ReturnTypeStandardScheme getScheme() {
      return new ReturnTypeStandardScheme();
    }
  }

  private static class ReturnTypeStandardScheme extends StandardScheme<ReturnType> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ReturnType struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // OUTPUT
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map34 = iprot.readMapBegin();
                struct.output = new HashMap<String,String>(2*_map34.size);
                for (int _i35 = 0; _i35 < _map34.size; ++_i35)
                {
                  String _key36;
                  String _val37;
                  _key36 = iprot.readString();
                  _val37 = iprot.readString();
                  struct.output.put(_key36, _val37);
                }
                iprot.readMapEnd();
              }
              struct.setOutputIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // DEP
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.dep = new Graph();
              struct.dep.read(iprot);
              struct.setDepIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, ReturnType struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.output != null) {
        oprot.writeFieldBegin(OUTPUT_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.output.size()));
          for (Map.Entry<String, String> _iter38 : struct.output.entrySet())
          {
            oprot.writeString(_iter38.getKey());
            oprot.writeString(_iter38.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.dep != null) {
        if (struct.isSetDep()) {
          oprot.writeFieldBegin(DEP_FIELD_DESC);
          struct.dep.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ReturnTypeTupleSchemeFactory implements SchemeFactory {
    public ReturnTypeTupleScheme getScheme() {
      return new ReturnTypeTupleScheme();
    }
  }

  private static class ReturnTypeTupleScheme extends TupleScheme<ReturnType> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ReturnType struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.output.size());
        for (Map.Entry<String, String> _iter39 : struct.output.entrySet())
        {
          oprot.writeString(_iter39.getKey());
          oprot.writeString(_iter39.getValue());
        }
      }
      BitSet optionals = new BitSet();
      if (struct.isSetDep()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetDep()) {
        struct.dep.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ReturnType struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TMap _map40 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
        struct.output = new HashMap<String,String>(2*_map40.size);
        for (int _i41 = 0; _i41 < _map40.size; ++_i41)
        {
          String _key42;
          String _val43;
          _key42 = iprot.readString();
          _val43 = iprot.readString();
          struct.output.put(_key42, _val43);
        }
      }
      struct.setOutputIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.dep = new Graph();
        struct.dep.read(iprot);
        struct.setDepIsSet(true);
      }
    }
  }

}

