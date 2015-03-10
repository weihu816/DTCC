include "RococoCommunicationService.thrift"
namespace java cn.ict.rcc.messaging

service RococoCoordinator {

  bool ping(),
  void callProcedure(1:string procedure, 2:list<string> paras)
  
}