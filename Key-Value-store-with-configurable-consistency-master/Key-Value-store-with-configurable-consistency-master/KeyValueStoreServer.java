import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

public class KeyValueStoreServer {

  public static KeyValueHandler handler;

  public static KeyValueService.Processor processor;

  public static void main(String [] args) {
    try {
      handler = new KeyValueHandler();
      System.out.println("args.lenght is : "+args.length);
      System.out.println("args are: " +args[0]+" "+args[1]+" "+args[2]+" "+args[3]);
      handler.setName(args[0]);
      handler.setReplicaList(args[1]);
      handler.setReplicaRepair(args[3]);
      processor = new KeyValueService.Processor(handler);
      // for(String str : args){
      //     System.out.println(str);
      // }
      int port = Integer.parseInt(args[2]);
      Runnable simple = new Runnable() {
        public void run() {
          simple(processor, port);
        }
      };
      new Thread(simple).start();
    } catch (Exception x) {
      x.printStackTrace();
    }
  }

  public static void simple(KeyValueService.Processor processor, int port ) {
    try {
      TServerTransport serverTransport = new TServerSocket(port);
      TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

      System.out.println("Starting the simple server...");
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
 
}