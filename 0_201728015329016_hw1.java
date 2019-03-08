import java.io.*;
import java.util.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.log4j.*;
/**  

* @author 201728015329016 wanglike

* @versionVersion 1.00

*/ 

public class Hw1Grp0
{
public static void main(String[] args) throws IOException,MasterNotRunningException, ZooKeeperConnectionException, URISyntaxException{

/*receive paramter and definition new which we will used
@file1 receive the path of table of R  
@file2 receive the path of table of S  
@Kjion receive the joinkey of R 
@jionK receive the joinkey of S
@ostr receive the put of join

*/
String file1= args[0].substring(2, args[0].length());   
String file2= args[1].substring(2, args[1].length()); 
int Kjion = Integer.parseInt(args[2].substring(6, 7));   
int jionK = Integer.parseInt(args[2].substring(9, 10));
String ostr = args[3].substring(4, args[3].length()); 
String astr[] = ostr.split(",");

int osum=astr.length;
int rsum = 0;
int ssum = 0;
int [] rnum = new int[20];
int [] snum = new int[20];

for(int i=0; i<astr.length ; i++ )
{
if((astr[i].substring(0,1)).equals("R")){
rnum[rsum]=Integer.parseInt(astr[i].substring(1,astr[i].length()));
rsum++;
}
}
for(int j=0; j<astr.length ; j++ )
{
if((astr[j].substring(0,1)).equals("S")){
snum[ssum]=Integer.parseInt(astr[j].substring(1,astr[j].length()));
ssum++;
}
}
//data to buffer
Configuration conf = new Configuration();
FileSystem fs = FileSystem.get(URI.create(file1), conf);
FSDataInputStream in_stream = fs.open(new Path(file1));
BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));

Configuration conf3 = new Configuration();
FileSystem fs3 = FileSystem.get(URI.create(file1), conf3);
FSDataInputStream in_stream3 = fs3.open(new Path(file1));
BufferedReader in3 = new BufferedReader(new InputStreamReader(in_stream3));

Configuration conf2 = new Configuration();
FileSystem fs2 = FileSystem.get(URI.create(file2), conf2);
FSDataInputStream in_stream2 = fs2.open(new Path(file2));
BufferedReader in2 = new BufferedReader(new InputStreamReader(in_stream2));


/*start put file1 data to hashtable ht
@s all data of file1 
@s2 all data of file2
@aline make sure zhe clom sums 
*/

String s;
String s2;   
String aline[]=in3.readLine().split("\\|");
Hashtable ht = new Hashtable();
String linkstr="";
String oldstr="";
while ((s=in.readLine())!=null) {
	
	aline=s.split("\\|");
	oldstr="";
	linkstr="";
	
	 if(ht.containsKey(aline[Kjion]))
	{
		oldstr = ht.get(aline[Kjion]).toString();
	
	}

	for(int k=0; k<rsum; k++)
	{

		if(linkstr=="")
		{linkstr=aline[rnum[k]];}
		else
		{linkstr=linkstr+"&"+aline[rnum[k]];}
	}

	if(oldstr!=""){
	linkstr = oldstr+"|"+linkstr;}
	
	
	ht.put(aline[Kjion],linkstr);

}
// start write

    Logger.getRootLogger().setLevel(Level.WARN);

    // create table descriptor
    String tableName= "result";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

    // create column descriptor
    HColumnDescriptor cf = new HColumnDescriptor("res");
    htd.addFamily(cf);

    // configure HBase
    Configuration configuration = HBaseConfiguration.create();
    HBaseAdmin hAdmin = new HBaseAdmin(configuration);

    if (hAdmin.tableExists(tableName)) {
        System.out.println("Table already exists");
	hAdmin.disableTable(tableName);
	hAdmin.deleteTable(tableName);
	System.out.println("The old Table has delete!");
    }
    
        hAdmin.createTable(htd);
        System.out.println("table "+tableName+ " created successfully");
    
    hAdmin.close();

HTable table = new HTable(configuration,tableName);

/*start join ht join s2
@splitstr every one in R
@flaght the is or not same of R
@aline make sure zhe clom sums the max same is 1000
@htsum the same num of all
*/


String pri;
String []splitstr = new String[1000];
Hashtable flaght = new Hashtable();
int htsum = 0;
String flagstr="";

while ((s2=in2.readLine())!=null) {
aline=s2.split("\\|");
if(ht.containsKey(aline[jionK]))
{
	

	splitstr=(ht.get(aline[jionK])).toString().split("\\|");

for(int l=0 ; l<splitstr.length ; l++ ){

	if(flaght.containsKey(aline[jionK]))
	{
	htsum = Integer.parseInt(flaght.get(aline[jionK]).toString());
	}
	else
	{
	htsum = 0;
	}

	if(htsum==0)
	{flagstr="";}
	else
	{flagstr="."+Integer.toString(htsum);}
	
	for(int y=0;y<rsum;y++)
	{
		Put put = new Put(aline[jionK].getBytes());
    		put.add("res".getBytes(),("R"+Integer.toString(rnum[y])+flagstr).getBytes(),splitstr[l].split("&")[y].getBytes());
    		table.put(put);}

	for(int z=0;z<ssum;z++){

		Put put2 = new Put(aline[jionK].getBytes());
		put2.add("res".getBytes(),("S"+Integer.toString(snum[z])+flagstr).getBytes(),aline[z].getBytes());
    		table.put(put2);}
	htsum = htsum+1;
	flaght.put(aline[jionK],Integer.toString(htsum));
	
}		
    		



}


}

table.close();
System.out.println("put successfully");
in.close();
fs.close();

}}




