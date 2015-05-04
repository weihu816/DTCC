namespace java cn.ict.rcc.messaging

enum Action {
	READROW 	 = 1,
    READSELECT 	 = 2,
    FETCHONE	 = 3,
    FETCHALL	 = 4,
    WRITE 		 = 5,
    ADDI 	 	 = 6,
    ADDF 	 	 = 7,
    REDUCEI		 = 8,
    DELETE 		 = 9
}

// below are for rococo
struct Vertex {
  1: required Action action
  2: optional list<string> name
  3: optional list<string> value
}
struct Piece {
  1: required list<Vertex> vertexs
  2: required string transactionId
  3: required string table
  4: required string key
  5: required bool immediate
  6: required i32 id
}
struct Edge {
  1: required string from
  2: required string to
  3: required bool immediate
}
struct Graph {
  1: required map<string, string> vertexes
  2: optional map<string, set<string>> serversInvolved
}
struct StartResponse {
  1: required list<string> output
  2: required Graph dep
}
struct StartResponseBulk {
  1: required list<list<string>> output
  2: required Graph dep
}
struct CommitResponse {
  1: bool result
  2: optional map<i32, list<string>> output
}

service RococoCommunicationService {

  bool ping(),
  
  // for rcc
  StartResponse start_req(1:Piece piece),
  StartResponseBulk start_req_bulk(1:list<Piece> pieces),
  CommitResponse commit_req(1:string transactionId, 2:Graph dep),
  
  // for communication between servers
  bool rcc_ask_txnCommitting(1:string transactionId),
  
  // for init
  bool write(1:string table, 2:string key, 3:list<string> names, 4:list<string> values),
  bool createSecondaryIndex(1:string table, 2:list<string> fields)
  
}