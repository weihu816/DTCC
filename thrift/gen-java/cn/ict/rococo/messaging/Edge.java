/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package cn.ict.rococo.messaging;

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

public class Edge implements org.apache.thrift.TBase<Edge, Edge._Fields>, java.io.Serializable, Cloneable, Comparable<Edge> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Edge");

  private static final org.apache.thrift.protocol.TField FROM_FIELD_DESC = new org.apache.thrift.protocol.TField("from", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField TO_FIELD_DESC = new org.apache.thrift.protocol.TField("to", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField IMMEDIATE_FIELD_DESC = new org.apache.thrift.protocol.TField("immediate", org.apache.thrift.protocol.TType.BOOL, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new EdgeStandardSchemeFactory());
    schemes.put(TupleScheme.class, new EdgeTupleSchemeFactory());
  }

  public String from; // required
  public String to; // required
  public boolean immediate; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FROM((short)1, "from"),
    TO((short)2, "to"),
    IMMEDIATE((short)3, "immediate");

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
        case 1: // FROM
          return FROM;
        case 2: // TO
          return TO;
        case 3: // IMMEDIATE
          return IMMEDIATE;
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
  private static final int __IMMEDIATE_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.FROM, new org.apache.thrift.meta_data.FieldMetaData("from", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TO, new org.apache.thrift.meta_data.FieldMetaData("to", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.IMMEDIATE, new org.apache.thrift.meta_data.FieldMetaData("immediate", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Edge.class, metaDataMap);
  }

  public Edge() {
  }

  public Edge(
    String from,
    String to,
    boolean immediate)
  {
    this();
    this.from = from;
    this.to = to;
    this.immediate = immediate;
    setImmediateIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Edge(Edge other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetFrom()) {
      this.from = other.from;
    }
    if (other.isSetTo()) {
      this.to = other.to;
    }
    this.immediate = other.immediate;
  }

  public Edge deepCopy() {
    return new Edge(this);
  }

  @Override
  public void clear() {
    this.from = null;
    this.to = null;
    setImmediateIsSet(false);
    this.immediate = false;
  }

  public String getFrom() {
    return this.from;
  }

  public Edge setFrom(String from) {
    this.from = from;
    return this;
  }

  public void unsetFrom() {
    this.from = null;
  }

  /** Returns true if field from is set (has been assigned a value) and false otherwise */
  public boolean isSetFrom() {
    return this.from != null;
  }

  public void setFromIsSet(boolean value) {
    if (!value) {
      this.from = null;
    }
  }

  public String getTo() {
    return this.to;
  }

  public Edge setTo(String to) {
    this.to = to;
    return this;
  }

  public void unsetTo() {
    this.to = null;
  }

  /** Returns true if field to is set (has been assigned a value) and false otherwise */
  public boolean isSetTo() {
    return this.to != null;
  }

  public void setToIsSet(boolean value) {
    if (!value) {
      this.to = null;
    }
  }

  public boolean isImmediate() {
    return this.immediate;
  }

  public Edge setImmediate(boolean immediate) {
    this.immediate = immediate;
    setImmediateIsSet(true);
    return this;
  }

  public void unsetImmediate() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __IMMEDIATE_ISSET_ID);
  }

  /** Returns true if field immediate is set (has been assigned a value) and false otherwise */
  public boolean isSetImmediate() {
    return EncodingUtils.testBit(__isset_bitfield, __IMMEDIATE_ISSET_ID);
  }

  public void setImmediateIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __IMMEDIATE_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case FROM:
      if (value == null) {
        unsetFrom();
      } else {
        setFrom((String)value);
      }
      break;

    case TO:
      if (value == null) {
        unsetTo();
      } else {
        setTo((String)value);
      }
      break;

    case IMMEDIATE:
      if (value == null) {
        unsetImmediate();
      } else {
        setImmediate((Boolean)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case FROM:
      return getFrom();

    case TO:
      return getTo();

    case IMMEDIATE:
      return Boolean.valueOf(isImmediate());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case FROM:
      return isSetFrom();
    case TO:
      return isSetTo();
    case IMMEDIATE:
      return isSetImmediate();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Edge)
      return this.equals((Edge)that);
    return false;
  }

  public boolean equals(Edge that) {
    if (that == null)
      return false;

    boolean this_present_from = true && this.isSetFrom();
    boolean that_present_from = true && that.isSetFrom();
    if (this_present_from || that_present_from) {
      if (!(this_present_from && that_present_from))
        return false;
      if (!this.from.equals(that.from))
        return false;
    }

    boolean this_present_to = true && this.isSetTo();
    boolean that_present_to = true && that.isSetTo();
    if (this_present_to || that_present_to) {
      if (!(this_present_to && that_present_to))
        return false;
      if (!this.to.equals(that.to))
        return false;
    }

    boolean this_present_immediate = true;
    boolean that_present_immediate = true;
    if (this_present_immediate || that_present_immediate) {
      if (!(this_present_immediate && that_present_immediate))
        return false;
      if (this.immediate != that.immediate)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(Edge other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetFrom()).compareTo(other.isSetFrom());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFrom()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.from, other.from);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTo()).compareTo(other.isSetTo());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTo()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.to, other.to);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetImmediate()).compareTo(other.isSetImmediate());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetImmediate()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.immediate, other.immediate);
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
    StringBuilder sb = new StringBuilder("Edge(");
    boolean first = true;

    sb.append("from:");
    if (this.from == null) {
      sb.append("null");
    } else {
      sb.append(this.from);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("to:");
    if (this.to == null) {
      sb.append("null");
    } else {
      sb.append(this.to);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("immediate:");
    sb.append(this.immediate);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (from == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'from' was not present! Struct: " + toString());
    }
    if (to == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'to' was not present! Struct: " + toString());
    }
    // alas, we cannot check 'immediate' because it's a primitive and you chose the non-beans generator.
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class EdgeStandardSchemeFactory implements SchemeFactory {
    public EdgeStandardScheme getScheme() {
      return new EdgeStandardScheme();
    }
  }

  private static class EdgeStandardScheme extends StandardScheme<Edge> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Edge struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // FROM
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.from = iprot.readString();
              struct.setFromIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TO
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.to = iprot.readString();
              struct.setToIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // IMMEDIATE
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.immediate = iprot.readBool();
              struct.setImmediateIsSet(true);
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
      if (!struct.isSetImmediate()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'immediate' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, Edge struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.from != null) {
        oprot.writeFieldBegin(FROM_FIELD_DESC);
        oprot.writeString(struct.from);
        oprot.writeFieldEnd();
      }
      if (struct.to != null) {
        oprot.writeFieldBegin(TO_FIELD_DESC);
        oprot.writeString(struct.to);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(IMMEDIATE_FIELD_DESC);
      oprot.writeBool(struct.immediate);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class EdgeTupleSchemeFactory implements SchemeFactory {
    public EdgeTupleScheme getScheme() {
      return new EdgeTupleScheme();
    }
  }

  private static class EdgeTupleScheme extends TupleScheme<Edge> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Edge struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.from);
      oprot.writeString(struct.to);
      oprot.writeBool(struct.immediate);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Edge struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.from = iprot.readString();
      struct.setFromIsSet(true);
      struct.to = iprot.readString();
      struct.setToIsSet(true);
      struct.immediate = iprot.readBool();
      struct.setImmediateIsSet(true);
    }
  }

}

