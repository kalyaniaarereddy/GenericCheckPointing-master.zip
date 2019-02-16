typedef i32 Key
typedef string Value
exception SystemException {
  1: optional string message
}
enum Consistency{
	QUORUM = 1,
	ONE = 2
}
struct Replica{
	1:string name,
	2:string ip,
	3:i32 port
}
struct Request{
	1: optional string timestamp,
	2: required bool coordinator
}
service KeyValueService
{
        bool put(1:Key key, 2:Value val, 3:Consistency cat, 4:Request request)
		throws (1: SystemException systemException),

		string get(1:Key key, 2:Consistency cat, 3:Request request)
		throws (1: SystemException systemException),

}