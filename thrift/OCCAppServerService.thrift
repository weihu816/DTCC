include "OCCCommunicationService.thrift"
namespace java cn.ict.occ.messaging

service OCCAppServerService {
  bool ping(),
  
  OCCCommunicationService.ReadValue read(1:string table, 2:string key, 3:list<string> names),
  
  OCCCommunicationService.ReadValue readIndexFetchTop(1:string table, 2:string keyIndex, 4:list<string> names, 5:string orderField, 6:bool isAssending),
  
  OCCCommunicationService.ReadValue readIndexFetchMiddle(1:string table, 2:string keyIndex, 3:string orderField, 4:bool isAssending),
  
  
//  list<OCCCommunicationService.ReadValue> read(1:string table, 2:string key_prefix, 3:string projectionColumn, 4:string constraintColumn, 5:i32 lowerBound, 6:i32 upperBound),
  
//  i32 read(1:string table, 2:string key_prefix, 3:string constraintColumn, 4:i32 lowerBound, 5:i32 upperBound),
  
  bool commit(1:string transactionId, 2:list<OCCCommunicationService.Option> options)
  
}