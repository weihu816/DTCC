namespace java cn.ict.occ.messaging

// below are for occ
struct ReadValue {
  1:i64 version
  2:list<string> values
}
struct Accept {
  1:string transactionId
  2:string table
  3:string key
  4:list<string> names
  5:list<string> newValues
  6:i64 oldVersion
}
struct Option {
  1:string table
  2:string key
  3:list<string> names
  4:list<string> values
  5:i64 oldVersion
}
service OCCCommunicationService {

  bool ping(),
  
  bool prepare(1:string table, 2:string key),

  bool accept(1:Accept accept),
  
  list<bool> bulkAccept(1:list<Accept> accepts),
    
  void decide(1:string transaction, 2:bool commit),
    
  ReadValue read(1:string table, 2:string key, 3:list<string> names),
  
  ReadValue readIndexFetchTop(1:string table, 2:string keyIndex, 3:list<string> names, 4:string orderField, 5:bool isAssending),
  
  ReadValue readIndexFetchMiddle(1:string table, 2:string keyIndex, 3:list<string> names, 4:string orderField, 5:bool isAssending),
  
  list<ReadValue> readIndexFetchAll(1:string table, 2:string keyIndex, 3:list<string> names),
  
  bool write(1:string table, 2:string key, 3:list<string> names, 4:list<string> values),
  
  bool createSecondaryIndex(1:string table, 2:list<string> fields)
  
  //list<ReadValue> read4(1:string table, 2:string key_prefix, 3:string projectionColumn, 4:string constraintColumn, 5:i32 lowerBound, 6:i32 upperBound),
  
  //i32 read5(1:string table, 2:string key_prefix, 3:string constraintColumn, 4:i32 lowerBound, 5:i32 upperBound),

}