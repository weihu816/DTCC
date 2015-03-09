namespace java cn.ict.rcc.messaging

enum Action {
	READROW 	 = 1,
    READSELECT 	 = 2,
    READPROJECT  = 3,
    WRITE 		 = 4,
    ADDVALUE 	 = 6,
    REDUCEVALUE  = 7,
    DELETE 		 = 8 
}

struct Vertex {
  1: required Action action
  2: required list<string> name
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

//struct Node {
//  1: required string id
//  2: required bool immediate
//}

//struct Graph {
//  1: required map<string, list<Node>> vertexes
//}

struct Graph {
  1: required map<string, string> vertexes
}

struct ReturnType {
  1: required map<string, string> output
  2: optional Graph dep
}

service RococoCommunicationService {
  bool ping(),
  ReturnType start_req(1:Piece piece),
  ReturnType commit_req(1:string transactionId, 2:Graph dep),
  bool write(1:string table, 2:string key, 3:list<string> names, 4:list<string> values)
}