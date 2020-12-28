package org.alex.hbasetest;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.BasicConfigurator;

public class HelloHBase {

	  public static void main( String[] args ) throws URISyntaxException, IOException
      {
	        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
            BasicConfigurator.configure();
	        Configuration config=HBaseConfiguration.create();
	        config.set("hbase.rootdir","hdfs://localhost:9000/hbase");
          config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

          config.addResource(
				  new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
		  config.addResource(
				  new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
          Connection connection=ConnectionFactory.createConnection(config);

          TableName tableName=TableName.valueOf("ZSCG");
          HTableDescriptor table=new HTableDescriptor(tableName);
          //HColumnDescriptor mycf = new HColumnDescriptor("info" );
        //  table.addFamily(new HColumnDescriptor(mycf));
          Admin admin=connection.getAdmin();
//////////////////////4444444444444444444444
          scanValue(connection,tableName,"01");
            ///////test function///////////////33333333333333333333
         // addValue(connection,tableName,"04","info","name","fsssj");    ////add     111111
            //addValue(connection,tableName,"02","grade","Chinese","78");    /////add 11111
           // checkValue(connection,tableName,"01");                  /////cheack and add   22222
         // imcrementValue(connection,tableName,"01");                   //////Increment 2L; 33333
         // getValue(connection,tableName,"01");                  /////////get    444444
           //deleteValue(connection,tableName,"01");          /////////delete   555555555



/////////////////////////22222222222222222
            // createSchemaTables(admin,tableName,table);                    ///create table
         // modifySchema(admin,tableName,table);                            //modify talbe
         // createOrOverWrite(admin,tableName,table);                       ////overwrites table
//          admin.createTable(table);
          admin.close();
          connection.close();

	    }
	    public static void createOrOverWrite(Admin admin,TableName tableName,HTableDescriptor table) throws IOException {
	      System.out.print("Creating table.");
        if (admin.tableExists(tableName))
        {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
      //  admin.createTable(table);

        }


    public static void createSchemaTables(Admin admin,TableName tableName,HTableDescriptor table) throws IOException{
        createOrOverWrite(admin,tableName,table);

        HColumnDescriptor mycf = new HColumnDescriptor("info" );
        table.addFamily(new HColumnDescriptor(mycf));
        admin.createTable(table);
    }

    public static void modifySchema(Admin admin,TableName tableName,HTableDescriptor table) throws IOException {
     //   if (!admin.tableExists(tableName))
     //   {
            HColumnDescriptor mycf = new HColumnDescriptor("grade" );
            table.addFamily(new HColumnDescriptor(mycf));
            admin.addColumn(tableName,mycf);
        HColumnDescriptor mycf1 = new HColumnDescriptor("info" );
        mycf1.setCompactionCompressionType(Compression.Algorithm.GZ);
        admin.modifyColumn(tableName,mycf1);
        // }

	  }
    public static void deleteSchema(Admin admin,TableName tableName,HTableDescriptor table) throws IOException {
        if (admin.tableExists(tableName))
        {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

	  }
    public static void addValue(Connection conn,TableName tableName,String rowkey,String colFamiy,String column,String colValue) throws IOException {
   Table table =conn.getTable(tableName);
    Put put=new Put(Bytes.toBytes(rowkey));
    put.addColumn(Bytes.toBytes(colFamiy),Bytes.toBytes(column),Bytes.toBytes(colValue));
    table.put(put);
        System.out.printf("finish add ");

        table.close();
	  }
    public static void checkValue(Connection conn,TableName tableName,String rowkey) throws IOException {
        Table table =conn.getTable(tableName);
        Put put=new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("Miky"));
        boolean result=table.checkAndPut(Bytes.toBytes(rowkey),Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("Jacky"),put);
	 // table.put(put);

        table.close();
	  }


    public static void imcrementValue(Connection conn,TableName tableName,String rowkey) throws IOException {
Table table=conn.getTable(tableName);
Put put = new Put(Bytes.toBytes(rowkey));
put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("18"));
table.put(put);
Increment inc = new Increment(Bytes.toBytes(rowkey));
inc.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),2);
table.increment(inc);
System.out.println("Done");

        table.close();
    }
    public static void getValue(Connection conn,TableName tableName,String rowkey) throws IOException {
        Table table=conn.getTable(tableName);
Get get =new Get(Bytes.toBytes(rowkey));
get.setMaxVersions(10);
Result result = table.get(get);
List<Cell> cells=result.getColumnCells(Bytes.toBytes("info"),Bytes.toBytes("name"));
for(Cell c:cells)
{
    byte[] cValue=CellUtil.cloneValue(c);
    System.out.println(Bytes.toString(cValue));
}
        table.close();
	  }
    public static void deleteValue(Connection conn,TableName tableName,String rowkey) throws IOException {
	      Table table=conn.getTable(tableName);
	      Delete delete1=new Delete(Bytes.toBytes(rowkey));
	      delete1.addColumn(Bytes.toBytes("grade"),Bytes.toBytes("Chinese"));
	      table.delete(delete1);
	      table.close();

    }
    public static void scanValue(Connection conn,TableName tableName,String rowkey) throws IOException {
	      Table table= conn.getTable(tableName);
	    //  Scan scan = new Scan (Bytes.toBytes(rowkey));
        Scan scan = new Scan ();
        ////////SingleColumnValueFilter
        Filter filter=new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL, new SubstringComparator("ma"));
        /////////ValueFilter
        Filter filter1=new ValueFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("ma1"));
        //////////rowfilter
        Filter filter2=new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes("01")));
        scan.setFilter(filter2);
	      ResultScanner rs=table.getScanner(scan);
	      for(Result r:rs)
          {
              String name = Bytes.toString(r.getValue(Bytes.toBytes("info"),Bytes.toBytes("name")));
              System.out.println(name);
          }
	      rs.close();
//          {
//              for(Cell cell:r.rawCells())
//              {
//                  System.out.println(new String(CellUtil.cloneRow(cell))+":"+new String(CellUtil.cloneFamily(cell))
//                  +":"+new String(CellUtil.cloneQualifier(cell))+":"+new String(CellUtil.cloneValue(cell))+":"+cell.getTimestamp());
//              }
//          }
    }
    }
