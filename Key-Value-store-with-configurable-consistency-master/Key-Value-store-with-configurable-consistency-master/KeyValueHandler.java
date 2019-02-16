import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import java.net.InetAddress;
import java.io.BufferedReader;
import java.sql.Timestamp;
import java.util.*;
import java.io.*;
import java.net.UnknownHostException;


public class KeyValueHandler implements KeyValueService.Iface {
    private Map<Integer, String> valueStore = null;
    private List<Replica> replicaList = null;
    private FileProcessor fp  = null;
    private String name;
    private String replicaRepair;
    File file = null;
    File file2 = null;
    int replica_count ;

    public KeyValueHandler() {
        valueStore = new HashMap<>();
        fp = new FileProcessor();
        replicaList = new ArrayList<>();
        file = new File("log.txt");
        file2 = new File("hints.txt");
    }

    public List<Replica> getReplicaList() {
        return replicaList;
    }

    public void setReplicaList(List<Replica> replicaList) {
        this.replicaList = replicaList;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public String getReplicaRepair() {
        return name;
    }

    public void setReplicaRepair(String name) {
        this.replicaRepair = name;
    }
    public void setReplicaList(String name){
        BufferedReader reader;
        try {
            String line;
            reader = fp.readerDesc(name);
            while ((line = fp.readLine(reader)) != null) {
                Replica rp = new Replica();
                String[] elements = line.split(" ");
                rp.setName(elements[0]);
                rp.setIp(elements[1]);
                rp.setPort(Integer.parseInt(elements[2]));
                System.out.println(line);
                replicaList.add(rp);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("file reader");
            System.exit(0);
        } finally {

        }
    }
    


    public void logFile(int key, String val)
    {
        FileWriter fw = null;
        BufferedWriter bw = null;
        String timestamp;
        try
        {
            fw = new FileWriter(file);
            bw = new BufferedWriter(fw);
            Date date = new Date();
            long time = date.getTime();
            timestamp = (new Timestamp(time)).toString() ;
            bw.write(key+" "+val+" "+timestamp+"\n");
            bw.close();
        }
        catch(IOException e)
        {
            System.out.println("exception");
        }
    }



public void updateAllReplicas(int key, String val,Consistency con, Request rs ,String ipIn, int portIn, String coordRep)
{
        int reached_consistency=0;
        int required_consistency;
        int total_consistency =3;
        int reached_inside =0;
        boolean done = false;
        switch(con)
        {
            case QUORUM : required_consistency = 2; break;
            case ONE : required_consistency =1; break;
            default: required_consistency =1; 
        }

        
        String name = this.getName();
        
        while(done==false)
        {
           for(Replica rl : getReplicaList())
           {
            
              if (((rl.getIp().equals(ipIn) && rl.getPort()==portIn) || reached_inside == 1 ) && done==false)  
              {
                    reached_inside = 1;
                    if(!rl.getIp().equals(coordRep))
                    {
                        
                        
                        try 
                        {
                            TTransport transport;
                            transport = new TSocket(rl.getIp(), rl.getPort());
                            transport.open();
                            TProtocol protocol = new  TBinaryProtocol(transport);
                            KeyValueService.Client client = new KeyValueService.Client(protocol);
                            boolean success = client.put(key,val,con,rs);
                            transport.close();
                            if(success == true)
                                reached_consistency++;
                    
                        } 
                        catch (TException x) 
                        {
                            x.printStackTrace();
                        }
                    }//end of internal if
                    else
                    {

                        if(this.getReplicaRepair().equals("hinted-handoff"))
                        {
                            //CHECK THE HINTS FILE IF THERE ARE ANY HINTS
                            FileReader fr3 = null;
                            BufferedReader br3 = null;
                            FileWriter fr4 = null;
                            BufferedWriter br4 = null;
                            FileReader fr5 = null;
                            BufferedReader br5 = null;
                            FileWriter fr6 = null;
                            BufferedWriter br6 = null;

                            try
                            {
                                fr3 = new FileReader(file2);
                                br3 = new BufferedReader(fr3); 
                                fr4 = new FileWriter("temp.txt");
                                br4 = new BufferedWriter(fr4);
                                fr5 = new FileReader("temp.txt");
                                br5 = new BufferedReader(fr5);
                                fr6 = new FileWriter(file2);
                                br6 = new BufferedWriter(fr6);

                                String line3;
                                while((line3 = br3.readLine())!=null)
                                {
                                    boolean taken = false;
                                    String sp[] = line3.split(" ");
                                    for(Replica r8 : getReplicaList())
                                    {
                                        if(r8.getIp().equals(sp[0]))
                                        {
                        
                                            try 
                                            {
                                                TTransport transport;
                                                transport = new TSocket(r8.getIp(), r8.getPort());
                                                transport.open();
                                                TProtocol protocol = new  TBinaryProtocol(transport);
                                                KeyValueService.Client client = new KeyValueService.Client(protocol);        
                                                if(transport.isOpen()==true)
                                                    taken = client.put(Integer.parseInt(sp[1]), sp[2], con, rs);
                                                transport.close();
                                            } 
                                            catch (TException x) 
                                            {
                                                x.printStackTrace();
                                            }
                                        }//end of if

                                    }    //end of for
                                    if(taken == false) //if the key-value cannot be put in right replica, put that line into another temporary file
                                    {
                                        br4.write(line3);
                                    }
                                }//end of while
                                //br3.flush(); //empty the hints.txt file
                                br3.close();
                                br4.close();
                                String line4;
                                while((line4=br5.readLine())!=null) //copying temporary file ti hints file
                                {
                                    br6.write(line4);
                                }


                            } //end of try
                            catch(IOException e)
                            {
                                System.out.println(e);
                            }

                        }//end of if---hinted-handoff


                        logFile(key,val);
                        valueStore.put(key,val);
                        reached_consistency++;
                        
                    } //end of else




                    class remReplicas implements Runnable 
                    {
                        int key;
                        remReplicas(int keyIn, String val, Consistency con, Request rs) 
                        { 
                            key = keyIn;
                        }
                        public void run() 
                        {
                            
                            int i=0;
                            int j=0;
                            switch(con)
                            {
                                case ONE:
                                    if(key>=0 && key<=63)
                                    {
                                        i=1; j=2;
                                    }
                                    else if(key>=64 && key<=127)
                                    {
                                        i=2; j=3;
                                    }
                                    else if(key>=128 && key<=191)
                                    {
                                        i=3; j=0;
                                    }
                                    else if(key>=192 && key<=255)
                                    {
                                        i=0; j=1;
                                    }
                                    int k=0;
                                    for(Replica rl : getReplicaList())
                                    {
                                        if(k==i ||k==j)
                                        {
                                            try 
                                            {
                                                TTransport transport;
                                                transport = new TSocket(rl.getIp(), rl.getPort());
                                                transport.open();
                                                TProtocol protocol = new  TBinaryProtocol(transport);
                                                KeyValueService.Client client = new KeyValueService.Client(protocol);
                                                rs.setCoordinator(false);
                                                boolean success = client.put(key,val,con,rs);
                                                transport.close();
                    
                                            } 
                                            catch (TException x) 
                                            {
                                                x.printStackTrace();
                                            }     
                                        }
                                           
                                        ++k;
                                    }//end of for loop
                                    break;

                                case QUORUM:
                                    if(key>=0 && key<=63)
                                    {
                                        i=2; 
                                    }
                                    else if(key>=64 && key<=127)
                                    {
                                        i=3; 
                                    }
                                    else if(key>=128 && key<=191)
                                    {
                                        i=0; 
                                    }
                                    else if(key>=192 && key<=255)
                                    {
                                        i=1; 
                                    }
                                    int l=0;
                                    for(Replica rl : getReplicaList())
                                    {
                                        if(l==i)
                                        {
                                            try 
                                            {
                                                TTransport transport;
                                                transport = new TSocket(rl.getIp(), rl.getPort());
                                                transport.open();
                                                TProtocol protocol = new  TBinaryProtocol(transport);
                                                KeyValueService.Client client = new KeyValueService.Client(protocol);
                                                rs.setCoordinator(false);
                                                boolean success = client.put(key,val,con,rs);
                                                transport.close();
                    
                                            } 
                                            catch (TException x) 
                                            {
                                                x.printStackTrace();
                                            }     
                                        }
                                           
                                        ++l;
                                    }//end of for loop
                                    break;
                                default:
                                        System.out.println("in default");
                            }
                           

                        }
                    }
                    


                    if(reached_consistency == required_consistency)
                    {
                        done = true;
                        Thread t = new Thread(new remReplicas(key, val, con, rs));
                        t.start();
                        return;
                        
                    }
                    

                }//end of if
            
            } //end of for
        }//end of while    
}

        


    @Override
    public boolean put(int key, String val, Consistency con, Request rs) throws TException
    {
        
        String name = this.getName();

        String ip = null;
        try
        {
            InetAddress inet=InetAddress.getLocalHost();
            ip=inet.getHostAddress();
            
        }
        catch(UnknownHostException e)
        {
            System.out.println(e);
        }
        
        int i=0;
        if(rs.isCoordinator())
        {
            
            rs.setCoordinator(false);
            
            for(Replica rl : getReplicaList())
            { 
                
                if((i == 0) && (key>=0 && key<=63))  
                {
                    
                    updateAllReplicas(key,val,con,rs,rl.getIp(), rl.getPort(),ip);
                    i++;
                    
                }
                else if((i == 1) && (key>=64 && key<=127))  
                {
                    
                    updateAllReplicas(key,val,con,rs,rl.getIp(), rl.getPort(),ip);
                    i++;
                    
                }
                else if((i == 2) && (key>=128 && key<=191))  
                {
                    
                    updateAllReplicas(key,val,con,rs,rl.getIp(), rl.getPort(),ip);
                    i++;
                   
                }
                else if((i == 3) && (key>=192 && key<=255))  
                {
                    
                    updateAllReplicas(key,val,con,rs,rl.getIp(), rl.getPort(),ip);
                    i++;
                    
                }
                else 
                    i++;
                
                

            } //end of for

        }
        else
        {
            
            if(this.getReplicaRepair().equals("hinted-handoff"))
            {
                //CHECK THE HINTS FILE IF THERE ARE ANY HINTS
                FileReader fr3 = null;
                BufferedReader br3 = null;
                FileWriter fr4 = null;
                BufferedWriter br4 = null;
                FileReader fr5 = null;
                BufferedReader br5 = null;
                FileWriter fr6 = null;
                BufferedWriter br6 = null;

                try
                {
                    fr3 = new FileReader(file2);
                    br3 = new BufferedReader(fr3); 
                    fr4 = new FileWriter("temp.txt");
                    br4 = new BufferedWriter(fr4);
                    fr5 = new FileReader("temp.txt");
                    br5 = new BufferedReader(fr5);
                    fr6 = new FileWriter(file2);
                    br6 = new BufferedWriter(fr6);

                    String line3;
                    while((line3 = br3.readLine())!=null)
                    {
                        boolean taken = false;
                        String sp[] = line3.split(" ");
                        for(Replica rl : getReplicaList())
                        {
                            if(rl.getIp().equals(sp[0]))
                            {
                        
                                try 
                                {
                                    TTransport transport;
                                    transport = new TSocket(rl.getIp(), rl.getPort());
                                    transport.open();
                                    TProtocol protocol = new  TBinaryProtocol(transport);
                                    KeyValueService.Client client = new KeyValueService.Client(protocol);        
                                    if(transport.isOpen()==true)
                                        taken = client.put(Integer.parseInt(sp[1]),sp[2],con,rs);
                                    transport.close();
                                } 
                                catch (TException x) 
                                {
                                    x.printStackTrace();
                                }
                            }

                        }    //end of for
                        if(taken == false) //if the key-value cannot be put in right replica, put that line into another temporary file
                        {
                            br4.write(line3);
                        }
                    }//end of while

                   
                    br3.close();
                    br4.close();
                    String line4;

                    while((line4=br5.readLine())!=null) //copying temporary file ti hints file
                    {
                        br6.write(line4);
                    }


                } //end of try
                catch(IOException e)
                {
                    System.out.println(e);
                }

            } //end of if hinted-handoff

            logFile(key,val);
            valueStore.put(key,val);
            
        } //end of else loop
        return true;

     }
        



     @Override
    public String get(int key, Consistency con, Request rs) {
        
        String ip = null;
        try
        {
            InetAddress inet=InetAddress.getLocalHost();
            ip=inet.getHostAddress();
        }
        catch(UnknownHostException e)
        {
            System.out.println(e);
        }
        
        String val;
        

        int i=0;
        if(rs.isCoordinator())
        {
            rs.setCoordinator(false);
            val = readAllReplicas(key,con,rs,ip);
        }
        else
        {
            val = valueStore.get(key);
        }

        
        return val;
    }

    

public String readAllReplicas(int key, Consistency con, Request rs, String coordRep){
        //To do read all replicas
        String name = this.getName();
        HashMap<String,Integer> rep_key = new HashMap<>();
        List<String> values = new ArrayList<>();
        Map<String,String> val_time = new HashMap<>();
        HashMap<String,String> key_time2 = new HashMap<>();

        int i=0;


        int reached_consistency=0;
        int required_consistency;
        int reached_inside1 =0;
        int reached_inside2 =0;
        int reached_inside3 =0;
        int reached_inside4 =0;
        boolean done = false;
        switch(con)
        {
            case QUORUM : required_consistency = 3; break;
            case ONE : required_consistency =3; break;
            default: required_consistency =1; 
        }


        while(done==false)
        {
            for(Replica rl : getReplicaList())
            {

                if((((i == 0)&& (key>=0 && key<=63)) || reached_inside1 == 1) && done == false )
                {
                    
                    reached_inside1 = 1;
                    
                    if(!rl.getIp().equals(coordRep))
                    {
                        
                        try {

                            TTransport transport;
                            transport = new TSocket(rl.getIp(), rl.getPort());
                            transport.open();
                            TProtocol protocol = new  TBinaryProtocol(transport);
                            KeyValueService.Client client = new KeyValueService.Client(protocol);
                            String v=client.get(key,con,rs);
                            values.add(v);  //gets all the values for that key for all replicas
                            rep_key.put(rl.getName(),key); //put the replica servr name and key in hashmap to return replica recent value
                            FileReader fr10 = null;
                            BufferedReader br10 = null;
                            try
                            {
                                fr10 = new FileReader(file);
                                br10 = new BufferedReader(fr10);
                                String line10;
                                while((line10 = br10.readLine())!=null)
                                {
                                    String[] str10 = line10.split(" ");
                                    if(Integer.parseInt(str10[0]) == key)
                                    {
                                        key_time2.put(str10[0]+" "+str10[1],str10[3]); //putting value and time stamp into hashmap
                                    }   
                                }
                                br10.close();
                                      
                            }
                            catch(IOException e)
                            {
                                System.out.println(e);
                            }
                            transport.close();  
                            reached_consistency++; 
                            
                    
                        } 
                        catch (TException x) 
                        {
                            x.printStackTrace();
                        }
                    }
                    else
                    {
                        values.add(valueStore.get(key));  //gets all the values for that key for all replicas
                        rep_key.put(rl.getName(),key); //put the replica servr name and key in hashmap to return replica recent value
                        FileReader fr11 = null;
                        BufferedReader br11 = null;
                        try
                        {
                            fr11 = new FileReader(file);
                            br11 = new BufferedReader(fr11);
                            String line11;
                            while((line11 = br11.readLine())!=null)
                            {
                                String[] str11 = line11.split(" ");
                                if(Integer.parseInt(str11[0])== key)
                                {
                                    key_time2.put(str11[0]+" "+str11[1],str11[3]); //putting value and time stamp into hashmap
                                }   
                            }
                            br11.close();
                                      
                        }
                        catch(IOException e)
                        {
                            System.out.println(e);
                        }
                        reached_consistency++;
                        
                    }

                    i++;
                    if(reached_consistency == required_consistency)
                    done = true;
                }
                else if((((i == 1)&& (key>=64 && key<=127)) || reached_inside2 == 1)  && done == false )
                {
                    reached_inside2 = 1;
                    
                    if(!rl.getIp().equals(coordRep))
                    {
                        try {
                            TTransport transport;
                            transport = new TSocket(rl.getIp(), rl.getPort());
                            transport.open();
                            TProtocol protocol = new  TBinaryProtocol(transport);
                            KeyValueService.Client client = new KeyValueService.Client(protocol);
                            values.add(client.get(key,con,rs));  //gets all the values for that key for all replicas
                            rep_key.put(rl.getName(),key); //put the replica servr name and key in hashmap to return replica recent value
                            FileReader fr12 = null;
                            BufferedReader br12 = null;
                            try
                            {
                                fr12 = new FileReader(file);
                                br12 = new BufferedReader(fr12);
                                String line12;
                                while((line12 = br12.readLine())!=null)
                                {
                                    String[] str12 = line12.split(" ");
                                    if(Integer.parseInt(str12[0]) == key)
                                    {
                                        key_time2.put(str12[0]+" "+str12[1],str12[3]); //putting value and time stamp into hashmap
                                    }   
                                }
                                br12.close();
                                      
                            }
                            catch(IOException e)
                            {
                                System.out.println(e);
                            }
                            transport.close();  
                            reached_consistency++; 
                    
                        } 
                        catch (TException x) 
                        {
                            x.printStackTrace();
                        }
                    }
                    else
                    {
                        values.add(valueStore.get(key));  //gets all the values for that key for all replicas
                        rep_key.put(rl.getName(),key); //put the replica servr name and key in hashmap to return replica recent value
                        FileReader fr13 = null;
                        BufferedReader br13 = null;
                        try
                        {
                            fr13 = new FileReader(file);
                            br13 = new BufferedReader(fr13);
                            String line13;
                            while((line13 = br13.readLine())!=null)
                            {
                                String[] str13 = line13.split(" ");
                                if(Integer.parseInt(str13[0])==key)
                                {
                                    key_time2.put(str13[0]+" "+str13[1],str13[3]); //putting value and time stamp into hashmap
                                }   
                            }
                            br13.close();
                                      
                        }
                        catch(IOException e)
                        {
                            System.out.println(e);
                        }
                        reached_consistency++;
                    }
                    i++;
                    if(reached_consistency == required_consistency)
                    done = true;
                }
                else if((((i == 2)&& (key>=128 && key<=191)) || reached_inside3 == 1)  && done == false )
                {
                    reached_inside3 = 1;
                
                    if(!rl.getIp().equals(coordRep))
                    {
                        try {
                            TTransport transport;
                            transport = new TSocket(rl.getIp(), rl.getPort());
                            transport.open();
                            TProtocol protocol = new  TBinaryProtocol(transport);
                            KeyValueService.Client client = new KeyValueService.Client(protocol);
                            values.add(client.get(key,con,rs));  //gets all the values for that key for all replicas
                            rep_key.put(rl.getName(),key); //put the replica servr name and key in hashmap to return replica recent value
                            FileReader fr14 = null;
                            BufferedReader br14 = null;
                            try
                            {
                                fr14 = new FileReader(file);
                                br14 = new BufferedReader(fr14);
                                String line14;
                                while((line14 = br14.readLine())!=null)
                                {
                                    String[] str14 = line14.split(" ");
                                    if(Integer.parseInt(str14[0])==key)
                                    {
                                        key_time2.put(str14[0]+" "+str14[1],str14[3]); //putting value and time stamp into hashmap
                                    }   
                                }
                                br14.close();
                                      
                            }
                            catch(IOException e)
                            {
                                System.out.println(e);
                            }
                            transport.close();  
                            reached_consistency++; 
                    
                        } 
                        catch (TException x) 
                        {
                            x.printStackTrace();
                        }
                    }
                    else
                    {
                        values.add(valueStore.get(key));  //gets all the values for that key for all replicas
                        rep_key.put(rl.getName(),key); //put the replica servr name and key in hashmap to return replica recent value
                        FileReader fr15 = null;
                        BufferedReader br15 = null;
                        try
                        {
                            fr15 = new FileReader(file);
                            br15 = new BufferedReader(fr15);
                            String line15;
                            while((line15 = br15.readLine())!=null)
                            {
                                String[] str15 = line15.split(" ");
                                if(Integer.parseInt(str15[0])== key)
                                {
                                    key_time2.put(str15[0]+" "+str15[1],str15[3]); //putting value and time stamp into hashmap
                                }   
                            }
                            br15.close();
                                      
                        }
                        catch(IOException e)
                        {
                            System.out.println(e);
                        }
                        reached_consistency++;
                    }
                    i++;
                    if(reached_consistency == required_consistency)
                    done = true;
                }
                else if((((i == 3)&& (key>=192 && key<=255)) || reached_inside4 == 1)  && done == false )
                {
                    reached_inside4 = 1;
                    
                    if(!rl.getIp().equals(coordRep))
                    {
                        try {
                            TTransport transport;
                            transport = new TSocket(rl.getIp(), rl.getPort());
                            transport.open();
                            TProtocol protocol = new  TBinaryProtocol(transport);
                            KeyValueService.Client client = new KeyValueService.Client(protocol);
                            values.add(client.get(key,con,rs));  //gets all the values for that key for all replicas
                            rep_key.put(rl.getName(),key); //put the replica servr name and key in hashmap to return replica recent value
                            FileReader fr16 = null;
                            BufferedReader br16 = null;
                            try
                            {
                                fr16 = new FileReader(file);
                                br16 = new BufferedReader(fr16);
                                String line16;
                                while((line16 = br16.readLine())!=null)
                                {
                                    String[] str16 = line16.split(" ");
                                    if(Integer.parseInt(str16[0])== key)
                                    {
                                        key_time2.put(str16[0]+" "+str16[1],str16[3]); //putting value and time stamp into hashmap
                                    }   
                                }
                                br16.close();
                                      
                            }
                            catch(IOException e)
                            {
                                System.out.println(e);
                            }
                            transport.close();  
                            reached_consistency++; 
                    
                        } 
                        catch (TException x) 
                        {
                            x.printStackTrace();
                        }
                    } 
                    else
                    {
                        values.add(valueStore.get(key));  //gets all the values for that key for all replicas
                        rep_key.put(rl.getName(),key); //put the replica servr name and key in hashmap to return replica recent value
                        FileReader fr17 = null;
                            BufferedReader br17 = null;
                            try
                            {
                                fr17 = new FileReader(file);
                                br17 = new BufferedReader(fr17);
                                String line17;
                                while((line17 = br17.readLine())!=null)
                                {
                                    String[] str17 = line17.split(" ");
                                    if(Integer.parseInt(str17[0])== key)
                                    {
                                        key_time2.put(str17[0]+" "+str17[1],str17[3]); //putting value and time stamp into hashmap
                                    }   
                                }
                                br17.close();
                                      
                            }
                            catch(IOException e)
                            {
                                System.out.println(e);
                            }
                        reached_consistency++;
                    } 
                    i++;
                    if(reached_consistency == required_consistency)
                    done = true;
                }
                else 
                {
                    i++;
                    
                }

            }//end of for
        }//end of while
        

        
        boolean consistent = true;
        for (int k = 0; k < values.size(); k++)  //checks if the values at all replicas are consistent, if not readRepair them
        {
            
            for (int j = 0; j < values.size(); j++) 
            {
                
                if(values.get(k).equals(values.get(j)))
                {
                    continue;
                    
                }
                else
                {
                    consistent = false;
                    readrepair(key, con, rs,rep_key,coordRep,key_time2);
                }
            }

        
        }



    
        Iterator it = rep_key.entrySet().iterator();
        while (it.hasNext())   //checking for recent timestamp value after consistent value is maintained
        {
            Map.Entry pair = (Map.Entry)it.next();
            // System.out.println(pair.getKey() + " ========= " + pair.getValue());
            for(Replica rl : getReplicaList())
            {
                if (rl.getName().equals(pair.getKey())) 
                {
                    try {
                        TTransport transport;
                        transport = new TSocket(rl.getIp(), rl.getPort());
                        transport.open();
                        TProtocol protocol = new  TBinaryProtocol(transport);
                        KeyValueService.Client client = new KeyValueService.Client(protocol);
                        FileReader fr = null;
                        BufferedReader br = null;
                        try
                        {
                            fr = new FileReader(file);
                            br = new BufferedReader(fr);
                            String line;
                            while((line = br.readLine())!=null)
                            {
                                String[] str2 = line.split(" ");
                                if(Integer.parseInt(str2[0])==Integer.valueOf((int)pair.getValue()))
                                {
                                    val_time.put(str2[1],str2[3]); //putting value and time stamp into hashmap
                                }
                            }
                            br.close();
                                      
                        }
                        catch(IOException e)
                        {
                            System.out.println(e);
                        }


                        transport.close(); 
                    }//end of try
                    catch (TException x) {
                        x.printStackTrace();
                    }
                }//end of if
            } //end of for
            it.remove();
        } //end of while

        
        String final_val = null;
        for(Map.Entry<String, String> e1: val_time.entrySet())
        { 
            for(Map.Entry<String, String> e2: val_time.entrySet())
            {    
                if (e1.getValue().compareTo( e2.getValue())>0)
                {
                    final_val = e1.getKey();
                }
                else
                {
                    final_val = e2.getKey();
                }
            }
        }



        return final_val; //should return the recent timestamp replica value
    }


    public void readrepair(int key, Consistency con, Request rs, HashMap<String,Integer> rep_keyIn, String coordRep ,HashMap<String,String> key_time2)
    {
        
        String recent_keyvalue = null;
        int recent_key;
        String recent_value;
       

        for(Map.Entry<String,String> e1: key_time2.entrySet())  //this gives recent timestamp
        { 
            for(Map.Entry<String,String> e2: key_time2.entrySet())
            {    
                if (e1.getValue().compareTo( e2.getValue())>0)
                {
                    recent_keyvalue = e1.getKey();
                }
                else
                {
                    recent_keyvalue = e2.getKey();
                }
            }
        }

        String[] splitting = recent_keyvalue.split(" ");
        recent_key = Integer.parseInt(splitting[0]);
        recent_value = splitting[1];
        
            

            for(Replica rl : getReplicaList())
            {

                for (Map.Entry me : rep_keyIn.entrySet()) 
                {
                    
                    if (rl.getName().equals(me.getKey())) 
                    {
                        try 
                        {
                            TTransport transport;
                            transport = new TSocket(rl.getIp(), rl.getPort());
                            transport.open();
                            TProtocol protocol = new  TBinaryProtocol(transport);
                            KeyValueService.Client client = new KeyValueService.Client(protocol);



                            if(this.getReplicaRepair().equals("hinted-handoff"))
                            {
                                if(transport.isOpen()== false) //Checking if hinted handoff needs to be done
                                {
                                    for(Replica r2 : getReplicaList())
                                    {
                                        if (r2.getIp().equals(coordRep)) //searching for coordinator replica
                                        {
                                            transport.close();


                                            FileReader fr2 = null;
                                            BufferedReader br2 = null;
                                            try
                                            {
                                                fr2 = new FileReader(file);
                                                br2 = new BufferedReader(fr2);
                                                String line1;
                                                while((line1 = br2.readLine())!=null)
                                                {
                                                    String str[] = line1.split(":");
                                                    if(Integer.parseInt(str[0]) == key)
                                                    {
                                                        hintedHandoff(r2.getIp(),r2.getPort(),recent_key,recent_value); //calling method to perform hinted hanoff
                                                    }
                                                }
                                                br2.close();
                                            }
                                            catch(IOException e)
                                            {
                                                System.out.println(e);
                                            }
                                        
                                        } //end of if
                                    } //end of replica for loop
                                } //end of if

                            }  //end of if--hinted handoff

                            
                            logFile(recent_key,recent_value);
                            FileReader fr1 = null;
                            BufferedReader br1 = null;
                            try
                            {
                                fr1 = new FileReader(file);
                                br1 = new BufferedReader(fr1);
                                String line;
                                while((line = br1.readLine())!=null)
                                {
                                    String str[] = line.split(":");
                                    rs.setCoordinator(false);
                                    client.put(recent_key,recent_value,con,rs);
                                }
                                br1.close();
                                
                            }
                            catch(IOException e)
                            {
                                System.out.println(e);
                            }
                        
                            transport.close();
                    
                        } 
                        catch (TException x) 
                        {
                            x.printStackTrace();
                        }
                    }//end of if
                }
                
               
            }


    }



public void hintedHandoff(String ipIn,int portIn, int key, String val)
{

    String ip = null;
    try
    {
        InetAddress inet=InetAddress.getLocalHost();
        ip=inet.getHostAddress();
    }
    catch(UnknownHostException e)
    {
        System.out.println(e);
    }

    if(ipIn.equals(ip)) //checks if the current replica is the coordinator itself
    {
        FileWriter fw = null;
        BufferedWriter bw = null;
        try
        {
            fw = new FileWriter(file2);
            bw = new BufferedWriter(fw);
            bw.write(ipIn+" "+key+" "+val+"\n");
            bw.close();
        }
        catch(IOException e)
        {
            System.out.println("exception");
        }
    }

    else
    {
        for(Replica rl : getReplicaList()) //else iterate to go to coordinator replica
        {
            if (rl.getIp().equals(ipIn) )
            {
                try 
                {
                    TTransport transport;
                    transport = new TSocket(rl.getIp(), rl.getPort());
                    transport.open();
                    TProtocol protocol = new  TBinaryProtocol(transport);
                    KeyValueService.Client client = new KeyValueService.Client(protocol);
                    FileWriter fw = null;
                    BufferedWriter bw = null;
                    try
                    {
                        fw = new FileWriter(file2);
                        bw = new BufferedWriter(fw);
                        bw.write(ipIn+" "+key+" "+val+"\n");
                        bw.close();
                    }
                    catch(IOException e)
                    {
                        System.out.println("exception");
                    }
                    transport.close();
                }
                catch (TException x) 
                {
                    x.printStackTrace();
                }

            }//end of if
        }//end of for
    
    } //end of else
}
}



