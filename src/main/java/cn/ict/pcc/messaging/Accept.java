/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package cn.ict.pcc.messaging;

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

public class Accept implements org.apache.thrift.TBase<Accept, Accept._Fields>, java.io.Serializable, Cloneable, Comparable<Accept> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Accept");

  private static final org.apache.thrift.protocol.TField TRANSACTION_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("transactionId", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField TABLE_FIELD_DESC = new org.apache.thrift.protocol.TField("table", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField KEY_FIELD_DESC = new org.apache.thrift.protocol.TField("key", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField NAMES_FIELD_DESC = new org.apache.thrift.protocol.TField("names", org.apache.thrift.protocol.TType.LIST, (short)4);
  private static final org.apache.thrift.protocol.TField NEW_VALUES_FIELD_DESC = new org.apache.thrift.protocol.TField("newValues", org.apache.thrift.protocol.TType.LIST, (short)5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new AcceptStandardSchemeFactory());
    schemes.put(TupleScheme.class, new AcceptTupleSchemeFactory());
  }

  public String transactionId; // required
  public String table; // required
  public String key; // required
  public List<String> names; // required
  public List<String> newValues; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TRANSACTION_ID((short)1, "transactionId"),
    TABLE((short)2, "table"),
    KEY((short)3, "key"),
    NAMES((short)4, "names"),
    NEW_VALUES((short)5, "newValues");

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
        case 1: // TRANSACTION_ID
          return TRANSACTION_ID;
        case 2: // TABLE
          return TABLE;
        case 3: // KEY
          return KEY;
        case 4: // NAMES
          return NAMES;
        case 5: // NEW_VALUES
          return NEW_VALUES;
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TRANSACTION_ID, new org.apache.thrift.meta_data.FieldMetaData("transactionId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TABLE, new org.apache.thrift.meta_data.FieldMetaData("table", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.KEY, new org.apache.thrift.meta_data.FieldMetaData("key", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.NAMES, new org.apache.thrift.meta_data.FieldMetaData("names", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.NEW_VALUES, new org.apache.thrift.meta_data.FieldMetaData("newValues", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Accept.class, metaDataMap);
  }

  public Accept() {
  }

  public Accept(
    String transactionId,
    String table,
    String key,
    List<String> names,
    List<String> newValues)
  {
    this();
    this.transactionId = transactionId;
    this.table = table;
    this.key = key;
    this.names = names;
    this.newValues = newValues;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Accept(Accept other) {
    if (other.isSetTransactionId()) {
      this.transactionId = other.transactionId;
    }
    if (other.isSetTable()) {
      this.table = other.table;
    }
    if (other.isSetKey()) {
      this.key = other.key;
    }
    if (other.isSetNames()) {
      List<String> __this__names = new ArrayList<String>(other.names);
      this.names = __this__names;
    }
    if (other.isSetNewValues()) {
      List<String> __this__newValues = new ArrayList<String>(other.newValues);
      this.newValues = __this__newValues;
    }
  }

  public Accept deepCopy() {
    return new Accept(this);
  }

  @Override
  public void clear() {
    this.transactionId = null;
    this.table = null;
    this.key = null;
    this.names = null;
    this.newValues = null;
  }

  public String getTransactionId() {
    return this.transactionId;
  }

  public Accept setTransactionId(String transactionId) {
    this.transactionId = transactionId;
    return this;
  }

  public void unsetTransactionId() {
    this.transactionId = null;
  }

  /** Returns true if field transactionId is set (has been assigned a value) and false otherwise */
  public boolean isSetTransactionId() {
    return this.transactionId != null;
  }

  public void setTransactionIdIsSet(boolean value) {
    if (!value) {
      this.transactionId = null;
    }
  }

  public String getTable() {
    return this.table;
  }

  public Accept setTable(String table) {
    this.table = table;
    return this;
  }

  public void unsetTable() {
    this.table = null;
  }

  /** Returns true if field table is set (has been assigned a value) and false otherwise */
  public boolean isSetTable() {
    return this.table != null;
  }

  public void setTableIsSet(boolean value) {
    if (!value) {
      this.table = null;
    }
  }

  public String getKey() {
    return this.key;
  }

  public Accept setKey(String key) {
    this.key = key;
    return this;
  }

  public void unsetKey() {
    this.key = null;
  }

  /** Returns true if field key is set (has been assigned a value) and false otherwise */
  public boolean isSetKey() {
    return this.key != null;
  }

  public void setKeyIsSet(boolean value) {
    if (!value) {
      this.key = null;
    }
  }

  public int getNamesSize() {
    return (this.names == null) ? 0 : this.names.size();
  }

  public java.util.Iterator<String> getNamesIterator() {
    return (this.names == null) ? null : this.names.iterator();
  }

  public void addToNames(String elem) {
    if (this.names == null) {
      this.names = new ArrayList<String>();
    }
    this.names.add(elem);
  }

  public List<String> getNames() {
    return this.names;
  }

  public Accept setNames(List<String> names) {
    this.names = names;
    return this;
  }

  public void unsetNames() {
    this.names = null;
  }

  /** Returns true if field names is set (has been assigned a value) and false otherwise */
  public boolean isSetNames() {
    return this.names != null;
  }

  public void setNamesIsSet(boolean value) {
    if (!value) {
      this.names = null;
    }
  }

  public int getNewValuesSize() {
    return (this.newValues == null) ? 0 : this.newValues.size();
  }

  public java.util.Iterator<String> getNewValuesIterator() {
    return (this.newValues == null) ? null : this.newValues.iterator();
  }

  public void addToNewValues(String elem) {
    if (this.newValues == null) {
      this.newValues = new ArrayList<String>();
    }
    this.newValues.add(elem);
  }

  public List<String> getNewValues() {
    return this.newValues;
  }

  public Accept setNewValues(List<String> newValues) {
    this.newValues = newValues;
    return this;
  }

  public void unsetNewValues() {
    this.newValues = null;
  }

  /** Returns true if field newValues is set (has been assigned a value) and false otherwise */
  public boolean isSetNewValues() {
    return this.newValues != null;
  }

  public void setNewValuesIsSet(boolean value) {
    if (!value) {
      this.newValues = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TRANSACTION_ID:
      if (value == null) {
        unsetTransactionId();
      } else {
        setTransactionId((String)value);
      }
      break;

    case TABLE:
      if (value == null) {
        unsetTable();
      } else {
        setTable((String)value);
      }
      break;

    case KEY:
      if (value == null) {
        unsetKey();
      } else {
        setKey((String)value);
      }
      break;

    case NAMES:
      if (value == null) {
        unsetNames();
      } else {
        setNames((List<String>)value);
      }
      break;

    case NEW_VALUES:
      if (value == null) {
        unsetNewValues();
      } else {
        setNewValues((List<String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TRANSACTION_ID:
      return getTransactionId();

    case TABLE:
      return getTable();

    case KEY:
      return getKey();

    case NAMES:
      return getNames();

    case NEW_VALUES:
      return getNewValues();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TRANSACTION_ID:
      return isSetTransactionId();
    case TABLE:
      return isSetTable();
    case KEY:
      return isSetKey();
    case NAMES:
      return isSetNames();
    case NEW_VALUES:
      return isSetNewValues();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Accept)
      return this.equals((Accept)that);
    return false;
  }

  public boolean equals(Accept that) {
    if (that == null)
      return false;

    boolean this_present_transactionId = true && this.isSetTransactionId();
    boolean that_present_transactionId = true && that.isSetTransactionId();
    if (this_present_transactionId || that_present_transactionId) {
      if (!(this_present_transactionId && that_present_transactionId))
        return false;
      if (!this.transactionId.equals(that.transactionId))
        return false;
    }

    boolean this_present_table = true && this.isSetTable();
    boolean that_present_table = true && that.isSetTable();
    if (this_present_table || that_present_table) {
      if (!(this_present_table && that_present_table))
        return false;
      if (!this.table.equals(that.table))
        return false;
    }

    boolean this_present_key = true && this.isSetKey();
    boolean that_present_key = true && that.isSetKey();
    if (this_present_key || that_present_key) {
      if (!(this_present_key && that_present_key))
        return false;
      if (!this.key.equals(that.key))
        return false;
    }

    boolean this_present_names = true && this.isSetNames();
    boolean that_present_names = true && that.isSetNames();
    if (this_present_names || that_present_names) {
      if (!(this_present_names && that_present_names))
        return false;
      if (!this.names.equals(that.names))
        return false;
    }

    boolean this_present_newValues = true && this.isSetNewValues();
    boolean that_present_newValues = true && that.isSetNewValues();
    if (this_present_newValues || that_present_newValues) {
      if (!(this_present_newValues && that_present_newValues))
        return false;
      if (!this.newValues.equals(that.newValues))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(Accept other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetTransactionId()).compareTo(other.isSetTransactionId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTransactionId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.transactionId, other.transactionId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTable()).compareTo(other.isSetTable());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTable()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.table, other.table);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetKey()).compareTo(other.isSetKey());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetKey()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.key, other.key);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNames()).compareTo(other.isSetNames());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNames()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.names, other.names);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNewValues()).compareTo(other.isSetNewValues());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNewValues()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.newValues, other.newValues);
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
    StringBuilder sb = new StringBuilder("Accept(");
    boolean first = true;

    sb.append("transactionId:");
    if (this.transactionId == null) {
      sb.append("null");
    } else {
      sb.append(this.transactionId);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("table:");
    if (this.table == null) {
      sb.append("null");
    } else {
      sb.append(this.table);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("key:");
    if (this.key == null) {
      sb.append("null");
    } else {
      sb.append(this.key);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("names:");
    if (this.names == null) {
      sb.append("null");
    } else {
      sb.append(this.names);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("newValues:");
    if (this.newValues == null) {
      sb.append("null");
    } else {
      sb.append(this.newValues);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
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

  private static class AcceptStandardSchemeFactory implements SchemeFactory {
    public AcceptStandardScheme getScheme() {
      return new AcceptStandardScheme();
    }
  }

  private static class AcceptStandardScheme extends StandardScheme<Accept> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Accept struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TRANSACTION_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.transactionId = iprot.readString();
              struct.setTransactionIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TABLE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.table = iprot.readString();
              struct.setTableIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // KEY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.key = iprot.readString();
              struct.setKeyIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // NAMES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list8 = iprot.readListBegin();
                struct.names = new ArrayList<String>(_list8.size);
                for (int _i9 = 0; _i9 < _list8.size; ++_i9)
                {
                  String _elem10;
                  _elem10 = iprot.readString();
                  struct.names.add(_elem10);
                }
                iprot.readListEnd();
              }
              struct.setNamesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // NEW_VALUES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list11 = iprot.readListBegin();
                struct.newValues = new ArrayList<String>(_list11.size);
                for (int _i12 = 0; _i12 < _list11.size; ++_i12)
                {
                  String _elem13;
                  _elem13 = iprot.readString();
                  struct.newValues.add(_elem13);
                }
                iprot.readListEnd();
              }
              struct.setNewValuesIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, Accept struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.transactionId != null) {
        oprot.writeFieldBegin(TRANSACTION_ID_FIELD_DESC);
        oprot.writeString(struct.transactionId);
        oprot.writeFieldEnd();
      }
      if (struct.table != null) {
        oprot.writeFieldBegin(TABLE_FIELD_DESC);
        oprot.writeString(struct.table);
        oprot.writeFieldEnd();
      }
      if (struct.key != null) {
        oprot.writeFieldBegin(KEY_FIELD_DESC);
        oprot.writeString(struct.key);
        oprot.writeFieldEnd();
      }
      if (struct.names != null) {
        oprot.writeFieldBegin(NAMES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.names.size()));
          for (String _iter14 : struct.names)
          {
            oprot.writeString(_iter14);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.newValues != null) {
        oprot.writeFieldBegin(NEW_VALUES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.newValues.size()));
          for (String _iter15 : struct.newValues)
          {
            oprot.writeString(_iter15);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class AcceptTupleSchemeFactory implements SchemeFactory {
    public AcceptTupleScheme getScheme() {
      return new AcceptTupleScheme();
    }
  }

  private static class AcceptTupleScheme extends TupleScheme<Accept> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Accept struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetTransactionId()) {
        optionals.set(0);
      }
      if (struct.isSetTable()) {
        optionals.set(1);
      }
      if (struct.isSetKey()) {
        optionals.set(2);
      }
      if (struct.isSetNames()) {
        optionals.set(3);
      }
      if (struct.isSetNewValues()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetTransactionId()) {
        oprot.writeString(struct.transactionId);
      }
      if (struct.isSetTable()) {
        oprot.writeString(struct.table);
      }
      if (struct.isSetKey()) {
        oprot.writeString(struct.key);
      }
      if (struct.isSetNames()) {
        {
          oprot.writeI32(struct.names.size());
          for (String _iter16 : struct.names)
          {
            oprot.writeString(_iter16);
          }
        }
      }
      if (struct.isSetNewValues()) {
        {
          oprot.writeI32(struct.newValues.size());
          for (String _iter17 : struct.newValues)
          {
            oprot.writeString(_iter17);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Accept struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.transactionId = iprot.readString();
        struct.setTransactionIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.table = iprot.readString();
        struct.setTableIsSet(true);
      }
      if (incoming.get(2)) {
        struct.key = iprot.readString();
        struct.setKeyIsSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TList _list18 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.names = new ArrayList<String>(_list18.size);
          for (int _i19 = 0; _i19 < _list18.size; ++_i19)
          {
            String _elem20;
            _elem20 = iprot.readString();
            struct.names.add(_elem20);
          }
        }
        struct.setNamesIsSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TList _list21 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.newValues = new ArrayList<String>(_list21.size);
          for (int _i22 = 0; _i22 < _list21.size; ++_i22)
          {
            String _elem23;
            _elem23 = iprot.readString();
            struct.newValues.add(_elem23);
          }
        }
        struct.setNewValuesIsSet(true);
      }
    }
  }

}

