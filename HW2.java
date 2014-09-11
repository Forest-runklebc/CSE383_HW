                                                                /*
 * Blake Runkle
 * CSE383
 * Fall 2014
 *
 * Skeleton code came from: http://www.mysamplecode.com/2011/12/java-multithreaded-socket-server.html
 */

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Calendar;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;

public class HW2 {

        public static final int PORT = 3047;
        ServerSocket myServerSocket;
        boolean ServerOn = true;
        Logger logger = Logger.getLogger(HW2.class.getName());
        FileHandler fh;
        Calendar now = Calendar.getInstance();
        SimpleDateFormat formatter = new SimpleDateFormat(
                        "E yyyy.MM.dd '-' hh:mm:ss a zzz");

        public HW2() throws IOException {
                try {
                        fh = new FileHandler("HW2.log", 1000000000, 1, true);
                        logger.addHandler(fh);
                        SimpleFormatter formatter = new SimpleFormatter();
                        fh.setFormatter(formatter);
                } catch (SecurityException e) {
                }

				try {
                        myServerSocket = new ServerSocket(PORT);
                } catch (IOException ioe) {
                        logger.info("Could not connect to server port 3046");
                        // fh.close();
                        return;
                }

                logger.info("Server Started on:" + formatter.format(now.getTime()));
                // fh.close();

                // Successfully created Server Socket. Now wait for connections.
                while (ServerOn) {
                        try {
                                Socket clientSocket = myServerSocket.accept();
                                ClientHandler cliThread = new ClientHandler(clientSocket);
                                cliThread.start();
                        } catch (IOException ioe) {
                                logger.info("Exception encountered on accepting incoming connection");
                                // fh.close();
                        }

                }

        }

        public static void main(String[] args) throws IOException {
                new HW2();
        }

        /**
         * Client Handler Class
         *
         * @author Blake
         *
         */
        class ClientHandler extends Thread {
                Socket myClientSocket;

                public ClientHandler() {
                        super();
                }

                ClientHandler(Socket s) {
                        myClientSocket = s;
                        try {
                                s.setSoTimeout(5000);
                        } catch (SocketException e) {
                                logger.info("Error setting time out on client socket");
                                fh.close();
                                return;
                        }
                }

                public void run() {
                        // fh.flush();
                        DataInputStream dis = null;
                        DataOutputStream dos = null;

                        logger.info("Accepted Client Address - "
                                        + myClientSocket.getInetAddress().getHostName() + ":"
                                        + myClientSocket.getPort() + " - "
                                        + formatter.format(now.getTime()));
                        // fh.close();

                        try {

                                dis = new DataInputStream(myClientSocket.getInputStream());
                                dos = new DataOutputStream(myClientSocket.getOutputStream());

												byte greetingLength = dis.readByte();

                                if (greetingLength != 30) {
                                        logger.info("Greeting length was not 30");
                                        // fh.close();
                                        dos.write(0); // tell the client "No Data"
                                        return;
                                } else {
                                        byte[] incomingByteArray = new byte[3];

                                        for (int i = 0; i < 10; i++) {
                                                dis.read(incomingByteArray);

                                                if (incomingByteArray[0] != 0x10
                                                                || incomingByteArray[1] != 0x20
                                                                || incomingByteArray[2] != 0x30) {
                                                        logger.info("One of the bytes in the array was not correct");
                                                        // fh.close();
                                                        dos.write(0);
                                                        return;
                                                }
                                        }
                                        AwsScan1 dbscan = new AwsScan1();
                                        int totalItemsToSend = dbscan.itemCount();
                                        dos.writeInt(totalItemsToSend);
                                        int dbSize = dbscan.allItems.size();

                                        for(int i = 0; i < dbSize; i++) {
                                                dos.writeUTF(dbscan.allItems.get(i).get("Name").getS());
                                                dos.writeUTF(dbscan.allItems.get(i).get("url").getS());
                                        }
                                        dos.writeInt(-1); //indicate end of UTF stream
                                }

                        } catch (Exception e) {
                                logger.info("Error in Run() of ClientHandler");
                                // fh.close();
                                return;
                        } finally {
                                try {
                                        dis.close();
                                        dos.close();
                                        myClientSocket.close();
                                        System.out.println("Closed dis, dos, and myClientSocket");
                                        logger.info("Closed connection with Client: "
                                                        + myClientSocket.getInetAddress().getHostName()
                                                        + ":" + myClientSocket.getPort() + " - "
                                                        + formatter.format(now.getTime()));
                                        fh.flush();
                                        fh.close();

                                } catch (IOException ioe) {
                                        logger.info("Client disconnected");
                                        // fh.close();
                                }
                        }
                }

        }

class AwsScan1 {

        AmazonDynamoDBClient client;

        java.util.List<Map<java.lang.String,AttributeValue>> allItems;

        public AwsScan1() {
                Main();
        }

        public void Main() {

				//connect to aws
                try {
                        createClient();
                } catch (Exception err) {
                        System.err.println("Can't connect to aws");
                        return;
                }

                try {
                        //get list of Items
                        allItems = GetAllItems();
                }  catch (AmazonServiceException ase) {
                        System.err.println(ase.getMessage());
                }
        }

        /**
        client used to connect to aws
        **/
        private void createClient() throws IOException {

                AWSCredentials credentials = new PropertiesCredentials(
                                AwsScan1.class.getResourceAsStream("AwsCredentials.properties"));

                client = new AmazonDynamoDBClient(credentials);
                client.setEndpoint("dynamodb.us-east-1.amazonaws.com");
        }

        /**
        get list .
        Return as a list of values
        */
        private java.util.List<Map<String,AttributeValue>> GetAllItems() throws AmazonServiceException {
                ScanRequest scan = new ScanRequest();   //create a scan to get ALL results
                scan.setTableName("cse383-f14-hw2");    //set name of table

                ScanResult result =  client.scan(scan);

                return result.getItems();

        }

        /**
        iterate over list - each list item is itself a mapping of attribute name and value. value is also a Map
        */
        private void printAll(java.util.List<Map<String,AttributeValue>> allItems) throws AmazonServiceException{
                for (Map<String,AttributeValue> itemSet: allItems) {
                        printItem(itemSet);
                }

        }

        /**
        print out trip item - finally
        */
        private void printItem(java.util.Map<java.lang.String,AttributeValue> itemSet) throws AmazonServiceException {
                for (Map.Entry<String, AttributeValue> item : itemSet.entrySet()) {
                        String attributeName = item.getKey();
                        AttributeValue value = item.getValue();
                        System.out.println(attributeName + " "
                                        + (value.getS() == null ? "" : "S=[" + value.getS() + "]")
                                        + (value.getN() == null ? "" : "N=[" + value.getN() + "]")
                                        + (value.getB() == null ? "" : "B=[" + value.getB() + "]")
                                        + (value.getSS() == null ? "" : "SS=[" + value.getSS() + "]")
                                        + (value.getNS() == null ? "" : "NS=[" + value.getNS() + "]")
                                        + (value.getBS() == null ? "" : "BS=[" + value.getBS() + "] \n"));
                }

        }

        public int itemCount() {
                return allItems.size();
        }
}

}

-- INSERT --                                       