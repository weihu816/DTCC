include "RococoCommunicationService.thrift"
namespace java cn.ict.rococo.messaging

service RococoCoordinator {

  bool ping(),
  void NewOrder(1:i32 w_id, 2:i32 d_id),
  bool commit(1:string transactionId, 2:list<RococoCommunicationService.Piece> pieces)
  
}