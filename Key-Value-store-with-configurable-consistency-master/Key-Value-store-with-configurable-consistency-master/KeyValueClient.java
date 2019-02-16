import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class KeyValueClient {
  public static void main(String [] args) {
    try {
      TTransport transport;

      transport = new TSocket(args[0], Integer.parseInt(args[1]));
      transport.open();

      TProtocol protocol = new  TBinaryProtocol(transport);
      KeyValueService.Client client = new KeyValueService.Client(protocol);
      perform(client);
      transport.close();
    } catch (TException x) {
      x.printStackTrace();
    } 
  }

  private static void perform(KeyValueService.Client client) throws TException
  {
    Request rs = new Request();
    

    rs.setCoordinator(true);
    boolean res1 = client.put(1, "Value",Consistency.valueOf("ONE"), rs);
    System.out.println("res = " +  res1);
    rs.setCoordinator(true);
    String st1 = client.get(1,Consistency.valueOf("ONE"), rs);
    System.out.println("st value =" + st1);


     rs.setCoordinator(true); 
    boolean res = client.put(192, "Value",Consistency.valueOf("QUORUM"), rs);
    System.out.println("res = " +  res);
    rs.setCoordinator(true);
    String st = client.get(192,Consistency.valueOf("QUORUM"), rs);
    System.out.println("st value =" + st);
  }
}