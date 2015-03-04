include "RococoCommunicationService.thrift"
namespace java cn.ict.rcc.messaging

service RococoCoordinator {

  bool ping(),
  void procedure_newOrder(1:i32 w_id, 2:i32 d_id),
  void procedure_micro(),
  bool commit(1:string transactionId, 2:list<RococoCommunicationService.Piece> pieces)
  
}