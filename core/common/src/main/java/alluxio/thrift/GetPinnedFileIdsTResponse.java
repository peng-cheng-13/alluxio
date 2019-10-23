/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package alluxio.thrift;

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
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class GetPinnedFileIdsTResponse implements org.apache.thrift.TBase<GetPinnedFileIdsTResponse, GetPinnedFileIdsTResponse._Fields>, java.io.Serializable, Cloneable, Comparable<GetPinnedFileIdsTResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("GetPinnedFileIdsTResponse");

  private static final org.apache.thrift.protocol.TField PINNED_FILE_IDS_FIELD_DESC = new org.apache.thrift.protocol.TField("pinnedFileIds", org.apache.thrift.protocol.TType.SET, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new GetPinnedFileIdsTResponseStandardSchemeFactory());
    schemes.put(TupleScheme.class, new GetPinnedFileIdsTResponseTupleSchemeFactory());
  }

  private Set<Long> pinnedFileIds; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    PINNED_FILE_IDS((short)1, "pinnedFileIds");

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
        case 1: // PINNED_FILE_IDS
          return PINNED_FILE_IDS;
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
    tmpMap.put(_Fields.PINNED_FILE_IDS, new org.apache.thrift.meta_data.FieldMetaData("pinnedFileIds", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.SetMetaData(org.apache.thrift.protocol.TType.SET, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(GetPinnedFileIdsTResponse.class, metaDataMap);
  }

  public GetPinnedFileIdsTResponse() {
  }

  public GetPinnedFileIdsTResponse(
    Set<Long> pinnedFileIds)
  {
    this();
    this.pinnedFileIds = pinnedFileIds;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public GetPinnedFileIdsTResponse(GetPinnedFileIdsTResponse other) {
    if (other.isSetPinnedFileIds()) {
      Set<Long> __this__pinnedFileIds = new HashSet<Long>(other.pinnedFileIds);
      this.pinnedFileIds = __this__pinnedFileIds;
    }
  }

  public GetPinnedFileIdsTResponse deepCopy() {
    return new GetPinnedFileIdsTResponse(this);
  }

  @Override
  public void clear() {
    this.pinnedFileIds = null;
  }

  public int getPinnedFileIdsSize() {
    return (this.pinnedFileIds == null) ? 0 : this.pinnedFileIds.size();
  }

  public java.util.Iterator<Long> getPinnedFileIdsIterator() {
    return (this.pinnedFileIds == null) ? null : this.pinnedFileIds.iterator();
  }

  public void addToPinnedFileIds(long elem) {
    if (this.pinnedFileIds == null) {
      this.pinnedFileIds = new HashSet<Long>();
    }
    this.pinnedFileIds.add(elem);
  }

  public Set<Long> getPinnedFileIds() {
    return this.pinnedFileIds;
  }

  public GetPinnedFileIdsTResponse setPinnedFileIds(Set<Long> pinnedFileIds) {
    this.pinnedFileIds = pinnedFileIds;
    return this;
  }

  public void unsetPinnedFileIds() {
    this.pinnedFileIds = null;
  }

  /** Returns true if field pinnedFileIds is set (has been assigned a value) and false otherwise */
  public boolean isSetPinnedFileIds() {
    return this.pinnedFileIds != null;
  }

  public void setPinnedFileIdsIsSet(boolean value) {
    if (!value) {
      this.pinnedFileIds = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case PINNED_FILE_IDS:
      if (value == null) {
        unsetPinnedFileIds();
      } else {
        setPinnedFileIds((Set<Long>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case PINNED_FILE_IDS:
      return getPinnedFileIds();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case PINNED_FILE_IDS:
      return isSetPinnedFileIds();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof GetPinnedFileIdsTResponse)
      return this.equals((GetPinnedFileIdsTResponse)that);
    return false;
  }

  public boolean equals(GetPinnedFileIdsTResponse that) {
    if (that == null)
      return false;

    boolean this_present_pinnedFileIds = true && this.isSetPinnedFileIds();
    boolean that_present_pinnedFileIds = true && that.isSetPinnedFileIds();
    if (this_present_pinnedFileIds || that_present_pinnedFileIds) {
      if (!(this_present_pinnedFileIds && that_present_pinnedFileIds))
        return false;
      if (!this.pinnedFileIds.equals(that.pinnedFileIds))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_pinnedFileIds = true && (isSetPinnedFileIds());
    list.add(present_pinnedFileIds);
    if (present_pinnedFileIds)
      list.add(pinnedFileIds);

    return list.hashCode();
  }

  @Override
  public int compareTo(GetPinnedFileIdsTResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetPinnedFileIds()).compareTo(other.isSetPinnedFileIds());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPinnedFileIds()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.pinnedFileIds, other.pinnedFileIds);
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
    StringBuilder sb = new StringBuilder("GetPinnedFileIdsTResponse(");
    boolean first = true;

    sb.append("pinnedFileIds:");
    if (this.pinnedFileIds == null) {
      sb.append("null");
    } else {
      sb.append(this.pinnedFileIds);
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

  private static class GetPinnedFileIdsTResponseStandardSchemeFactory implements SchemeFactory {
    public GetPinnedFileIdsTResponseStandardScheme getScheme() {
      return new GetPinnedFileIdsTResponseStandardScheme();
    }
  }

  private static class GetPinnedFileIdsTResponseStandardScheme extends StandardScheme<GetPinnedFileIdsTResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, GetPinnedFileIdsTResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // PINNED_FILE_IDS
            if (schemeField.type == org.apache.thrift.protocol.TType.SET) {
              {
                org.apache.thrift.protocol.TSet _set206 = iprot.readSetBegin();
                struct.pinnedFileIds = new HashSet<Long>(2*_set206.size);
                long _elem207;
                for (int _i208 = 0; _i208 < _set206.size; ++_i208)
                {
                  _elem207 = iprot.readI64();
                  struct.pinnedFileIds.add(_elem207);
                }
                iprot.readSetEnd();
              }
              struct.setPinnedFileIdsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, GetPinnedFileIdsTResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.pinnedFileIds != null) {
        oprot.writeFieldBegin(PINNED_FILE_IDS_FIELD_DESC);
        {
          oprot.writeSetBegin(new org.apache.thrift.protocol.TSet(org.apache.thrift.protocol.TType.I64, struct.pinnedFileIds.size()));
          for (long _iter209 : struct.pinnedFileIds)
          {
            oprot.writeI64(_iter209);
          }
          oprot.writeSetEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class GetPinnedFileIdsTResponseTupleSchemeFactory implements SchemeFactory {
    public GetPinnedFileIdsTResponseTupleScheme getScheme() {
      return new GetPinnedFileIdsTResponseTupleScheme();
    }
  }

  private static class GetPinnedFileIdsTResponseTupleScheme extends TupleScheme<GetPinnedFileIdsTResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, GetPinnedFileIdsTResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetPinnedFileIds()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetPinnedFileIds()) {
        {
          oprot.writeI32(struct.pinnedFileIds.size());
          for (long _iter210 : struct.pinnedFileIds)
          {
            oprot.writeI64(_iter210);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, GetPinnedFileIdsTResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TSet _set211 = new org.apache.thrift.protocol.TSet(org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.pinnedFileIds = new HashSet<Long>(2*_set211.size);
          long _elem212;
          for (int _i213 = 0; _i213 < _set211.size; ++_i213)
          {
            _elem212 = iprot.readI64();
            struct.pinnedFileIds.add(_elem212);
          }
        }
        struct.setPinnedFileIdsIsSet(true);
      }
    }
  }

}

