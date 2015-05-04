namespace java cn.ict.pcc.messaging

// below are for pcc
struct ReadValue {
  2:list<string> values
}
struct Accept {
  1:string transactionId
  2:string table
  3:string key
  4:list<string> names
  5:list<string> newValues
}
struct Option {
  1:string table
  2:string key
  3:list<string> names
  4:list<string> values
}
service PCCCommunicationService {

  bool ping(),

  bool accept(1:Accept accept),
  
  list<bool> bulkAccept(1:list<Accept> accepts),
    
  void decide(1:string transaction, 2:bool commit),
    
  ReadValue read(1:string txnid, 2:string table, 3:string key, 4:list<string> names),
  
  bool write(1:string table, 2:string key, 3:list<string> names, 4:list<string> values),
  
  bool createSecondaryIndex(1:string table, 2:list<string> fields)
  
}