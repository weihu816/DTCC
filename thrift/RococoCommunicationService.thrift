namespace java cn.ict.rcc.messaging

enum Action {
	READROW 	 = 1,
    READSELECT 	 = 2,
    FETCHONE	 = 3,
    FETCHALL	 = 4,
    WRITE 		 = 5,
    ADDINTEGER 	 = 6,
    ADDDECIMAL 	 = 7,
    DELETE 		 = 8
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
  1: required list<map<string, string>> output
  2: required Graph dep
}

struct CommitResponse {
  1: bool result
  2: optional list<map<string, string>> output
}

service RococoCommunicationService {

  bool ping(),
  
  // for rcc
  StartResponse start_req(1:Piece piece),
  StartResponse start_req_bulk(1:list<Piece> pieces),
  CommitResponse commit_req(1:string transactionId, 2:Graph dep),
  
  // for communication between servers
  bool rcc_ask_txnCommitting(1:string transactionId),
  
  // for init
  bool write(1:string table, 2:string key, 3:list<string> names, 4:list<string> values),
  bool createSecondaryIndex(1:string table, 2:list<string> fields)
  
}