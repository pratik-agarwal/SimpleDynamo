package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.List;
import java.util.Scanner;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	public static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	public static final int SERVER_PORT = 10000;
	public static String myUri = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	String[] portlist = new String[]{"11108", "11112", "11116", "11120","11124"};
	private static String myPort;
	private static  String myNode;
	//private static boolean  starQueryResult = false;

	public class FailedData{
	    public String key;
	    public String value;
	    public FailedData(String key, String value)
        {
            this.key = key;
            this.value = value;
        }
    }
    private static List<FailedData> failedList = new ArrayList<FailedData>();

	public static class MyAvd{

		String myPort;
		String myHash;

		public MyAvd(String myPort, String myHash) {
			this.myPort = myPort;
			this.myHash = myHash;
		}
	}

	public class AvdCompare implements Comparator<MyAvd> {

		@Override
		public int compare(MyAvd lhs, MyAvd rhs) {
			return lhs.myHash.compareTo(rhs.myHash);
		}
	}

	private ArrayList<MyAvd> avds_list = new ArrayList <MyAvd>();
	public static MyAvd curr;

    public class Compare implements Comparator<String> {

        @Override
        public int compare(String lhs, String rhs) {

            String left = "", right = "";
            try {
//                  left = genHash(String.valueOf(Integer.parseInt(lhs)/2));
                    left = genHash(lhs);
//                  right = genHash(String.valueOf(Integer.parseInt(rhs)/2));
                    right = genHash(rhs);
                    return (left).compareTo(right);
            }catch (NoSuchAlgorithmException ne) {
                ne.printStackTrace();
            }
            return (left).compareTo(right);
        }
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        Log.e("Inside onCreate","Starting the func");
        try{
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myNode = portStr;
            final String myPortStr = String.valueOf((Integer.parseInt(portStr) * 2));
            myPort = myPortStr;
            Log.e("My Port------" , myPort);

            for( String myemuStr:portlist) {
                curr = new MyAvd(myemuStr, genHash(String.valueOf(Integer.parseInt(myemuStr)/2)));
                avds_list.add(curr);
            }
            Collections.sort(avds_list, new AvdCompare());
            Log.e("Sorted Array List:", "Printing the list");
            for (int i = 0; i < avds_list.size(); i++) {
                Log.e("List element: ",  avds_list.get(i).myPort);
            }
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            // Insert failed key value pair if exist.
            GetFailedKeys();
        } catch (Exception ex) {
            ex.printStackTrace();
            Log.e(TAG, "Can't create a ServerSocket");
        }
        return false;
    }

    private void GetFailedKeys()
    {
        Log.e("Inside failed keys fn-"," Getting the failed keys");
        try{
            for(int i =0; i < avds_list.size();i++) {
                if (!avds_list.get(i).myPort.equals(myPort)) {
                    Log.e("On create -" + myPort, "asking keys from -" + avds_list.get(i).myPort);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Get Keys:" + myPort + ": NA" + ":" + avds_list.get(i).myPort);
                }
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public static Uri getUri() {
        Uri.Builder builder = new Uri.Builder();
        builder.authority(myUri);
        builder.scheme("content");
        Uri uri = builder.build();
        return uri;
    }


    private class ServerTask extends AsyncTask<ServerSocket, Void, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            try {
                ServerSocket serverSocket = sockets[0];
                Scanner scr = null;
                PrintWriter ptr = null;
                while (true) {
                    Socket socket = serverSocket.accept();
                    scr = new Scanner(socket.getInputStream());
                    //ptr = new PrintWriter(socket.getOutputStream(), true);
                    String command = scr.nextLine();
                    String msg [] = command.split(":");
                    if (msg[0].equals("InsertKey")) {
                        Log.e("At server-" + myPort, " Insert - " +  msg[1]);
                        Log.e("Inside the Insert cmd", command);
                        String key = msg[1];
                        String value = msg[2];
                        DataOutputStream dos = new DataOutputStream(getContext().openFileOutput(key, Context.MODE_PRIVATE));
                        dos.writeUTF(value);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Inserted");
                    }
                    else if( msg[0].equals("Query"))
                    {
                        StringBuilder sb = new StringBuilder();
                        String columnNames[] = new String[]{"key", "value"};
                        Log.e("At server - " + myPort ," REquest for query from-" + msg[1]);
                        MatrixCursor matrixCursor = new MatrixCursor( new String [] {"key","value"});
                        for (File file : getContext().getFilesDir().listFiles()) {
                            DataInputStream dis = new DataInputStream(getContext().openFileInput(file.getName()));
                            String value = dis.readUTF();
                            sb.append(file.getName() + "-" + value + "##");
                        }
                        Log.e("At server","My Keys - " + sb.toString());
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF(sb.toString());
                        out.flush();
                    }
                    else if(msg[0].equals("SingleQuery"))
                    {
                        Cursor cursor =  getContext().getContentResolver().query(getUri(),null, msg[2], null, null);
                        StringBuilder sb = new StringBuilder();
                        while(cursor.moveToNext()) {
                            String retrieveKey = cursor.getString(cursor.getColumnIndex("key"));
                            String retrieveValue = cursor.getString(cursor.getColumnIndex("value"));
                            sb.append(retrieveKey + "\n" + retrieveValue + "\n");
                        }
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF(sb.toString());
                    }
                    else if( msg[0].equals("Get Keys")){
                        StringBuilder sb = new StringBuilder();
                        for(int i =0 ; i < failedList.size(); i++) {
                            sb.append(failedList.get(i).key + "-" + failedList.get(i).value + "##");
                        }
                        Log.e("At server-" + myPort , " Sending my failed keys-" + sb.toString() );
                        SendMyKeysToFailedPort(msg[1], sb.toString());
                    }
                    else if (msg[0].equals("Get Keys Result")) {
                        Log.e("At server- " + myPort, "Inserting failed keys");
                        String keys = msg[2];
                        if (!keys.equals("empty")) {
                            Log.e("At server-" + myPort, "Get key result keys received-" + keys);
                            Log.e("Keys are not null", keys);
                            String[] receivedMessageTokens = keys.split("##");
                            for (String keyvalue : receivedMessageTokens) {
                                String[] keyValueToken = keyvalue.split("-");
                                if (keyValueToken.length != 0) {
                                    Log.e("Key Value-", keyValueToken[0] + " -" + keyValueToken[1]);
                                    DataOutputStream dos = new DataOutputStream(getContext().openFileOutput(keyValueToken[0], Context.MODE_PRIVATE));
                                    dos.writeUTF(keyValueToken[1]);
                                }
                            }
                        }
                    }
                    else{
                        Log.e("At server", "Somethng else received at server");
                        Log.e("Different message-",command );
                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "I/O Exception");
            }
            return null;
        }
    }

    private void SendMyKeysToFailedPort(String failedPort, String keys)
    {
        try {
            if(keys.length() >1) {
                String message = "Get Keys Result:" + myPort + ":" + keys + ":" + failedPort;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            }
            else {
                String message = "Get Keys Result:" + myPort + ":" + "empty" + ":" + failedPort;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            }

            failedList.clear();
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {

            String temp = strings[0];
            Log.e("Mesage to send-" , temp);
            String[] msg = temp.split(":");
            try{
                String port = msg[3];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                socket.setSoTimeout(2000);
                PrintWriter ptr = new PrintWriter(socket.getOutputStream(), true);
                ptr.println(temp);
                ptr.flush();

                if( msg[0].equals("InsertKey")) {
                    DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                    String message = inputStream.readUTF();
                    if (message == null) {
                        Log.e("At client-" + myPort, "Inputstream is null");
                    }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (Exception e) {
                if(msg[0].equals("InsertKey")) {
                    Log.e("At port -" + myPort, "***** EXCEPTION OCCURED");
                    Log.e("Exception at function -", msg[0]);
                    failedList.add(new FailedData(msg[1], msg[2]));
                    Log.e("Failed list size", failedList.size() + "");
                }
                else if (msg[0].equals("Query"))
                {

                }
                e.printStackTrace();
            }
            return null;
        }
    }


    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

        if(selection.equals("@") || selection.equals("*")) {
            Log.v("Delete:", "Inside the delete");
            File files[] = getContext().getFilesDir().listFiles();
            for (File file : files) {
                file.delete();
            }
        }
        else {
            File files[] = getContext().getFilesDir().listFiles();
            for (File file : files) {
                if(file.getName().equals(selection))
                    file.delete();
            }
        }
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
            ArrayList<String> temp_list = new ArrayList<String>();
            String key = values.get("key").toString();
            String value = values.get("value").toString();
            String hashedKey = genHash(key);
            Log.e(TAG, "Insert request at port -" + myPort + "for key -" + key);
            temp_list.add("5554");
            temp_list.add("5556");
            temp_list.add("5558");
            temp_list.add("5560");
            temp_list.add("5562");
            //temp_list.add(key);
            Collections.sort(temp_list, new Compare());
            int ind = -1;
            for(int i =0 ; i < temp_list.size() ; i++) {
                if( hashedKey.compareTo(genHash(temp_list.get(i))) < 0 ) {
                    ind = i;
                    break;
                }
            }
            if( ind == -1) {
                ind =0;
            }
            List<Integer> indexes = retIndex(ind);
            Log.e("Index length-" , indexes.size() + "");
            Log.e("Key index - " + ind , " And it should be inserted in " + indexes.get(0).toString() + "-" + indexes.get(1) + " -" + indexes.get(2));
            for (int index : indexes) {
                if(temp_list.get(ind).equals(myNode)) {
                    Log.e("Insert in my node-", "Key-" + key);
                    Log.e("Insert in avd with","key = "+  genHash(key));
                    DataOutputStream dos = new DataOutputStream(getContext().openFileOutput(key, Context.MODE_PRIVATE));
                    dos.writeUTF(value);
                }
                String message = "InsertKey" + ":" + key + ":" + value + ":" + String.valueOf(Integer.parseInt(avds_list.get(index).myPort) );
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            }
            return null;
        }catch(Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public List<Integer> retIndex(int ind){
            List<Integer> portIndex  = new ArrayList();
            try {
                if (ind == 5) {
                    portIndex.add(0);
                    portIndex.add(1);
                    portIndex.add(2);
                }
                else if (ind == 4) {
                    portIndex.add(4);
                    portIndex.add(0);
                    portIndex.add(1);
                }
                else if (ind == 3) {
                    portIndex.add(3);
                    portIndex.add(4);
                    portIndex.add(0);
                }
                else {
                    portIndex.add(ind);
                    portIndex.add(ind + 1);
                    portIndex.add(ind + 2);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            return portIndex;
        }


	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {

        try {
            MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
            String columnNames[] = new String[]{"key", "value"};
            if (selection.equals("@")) {
                for (File file : getContext().getFilesDir().listFiles()) {
                    DataInputStream dis = new DataInputStream(getContext().openFileInput(file.getName()));
                    String value = dis.readUTF();
                    columnNames = new String[]{file.getName(), value};
                    matrixCursor.addRow(columnNames);
                }
                return matrixCursor;
            }
            else if (selection.equals("*")) {
                File files[] = getContext().getFilesDir().listFiles();
                for (File file : files) {
                    DataInputStream dis = new DataInputStream(getContext().openFileInput(file.getName()));
                    String value = dis.readUTF();
                    columnNames = new String[]{file.getName(), value};
                    matrixCursor.addRow(columnNames);
                }
               for(int i =0 ; i< avds_list.size();i++) {
                   try{
                        if( !avds_list.get(i).myPort.equals(myPort)) {
                            String message = "Query" + ":" + myPort + ":" + "NA" + ":" + String.valueOf(Integer.parseInt(avds_list.get(i).myPort));
                            Log.e("Message to send", message);
                            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds_list.get(i).myPort));
                            PrintWriter ptr = new PrintWriter(socket.getOutputStream(), true);
                            ptr.println(message);
                            ptr.flush();
                            DataInputStream scr = new DataInputStream(socket.getInputStream());
                            String receivedMessage = scr.readUTF();
                            Log.e("Received message -", receivedMessage);
                            scr.close();
                            socket.close();
                            // Log.e("At port-" + myPort , "REceived string from "+avds_list.get(i).myPort + "-" + receivedMessage );
                            String[] receivedMessageTokens = receivedMessage.split("##");
                            // Scanner scr = new Scanner(socket.getInputStream());
                            for (String keyAndValue : receivedMessageTokens) {
                                String[] keyAndValueTokens = keyAndValue.split("-");
                                Log.e("Key value-", keyAndValueTokens[0] + "-" + keyAndValueTokens[1]);
                                matrixCursor.addRow(new String[]{keyAndValueTokens[0], keyAndValueTokens[1]});
                            }
                        }
                   }catch (Exception ex) {
                       continue;
                   }
               }
               return matrixCursor;
            }
            else {
                //Single query
                int actualIndex = -1;
                try {
                    boolean fileRet = false;
                    Log.e("Single query", "Yo single query");
                    File files[] = getContext().getFilesDir().listFiles();
                    for (File file : files) {
                        if (file.getName().equals(selection)) {
                            fileRet = true;
                            Log.e("Info: ", "File found");
                            DataInputStream dis = new DataInputStream(getContext().openFileInput(file.getName()));
                            String val = dis.readUTF();
                            columnNames = new String[]{file.getName(), val};
                            matrixCursor.addRow(columnNames);
                            return matrixCursor;
                        }
                    }
                    if (fileRet == false) {
                        Log.e("Info", "File not found,ready to ping next");
                        String val = "";
                        String msg = "SingleQuery:" + myPort + ":" + selection;
                        for(int i =0 ; i < avds_list.size() ; i++) {
                            if(genHash(selection).compareTo(genHash(String.valueOf( Integer.parseInt( avds_list.get(i).myPort )/2))) <0 ) {
                                actualIndex = i;
                                break;
                            }
                        }
                        if(actualIndex == -1)
                            actualIndex =0;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds_list.get(actualIndex).myPort));
                        PrintWriter ptr = new PrintWriter(socket.getOutputStream(), true);
                        ptr.println(msg);
                        ptr.flush();
                        Scanner scr = new Scanner(socket.getInputStream());
                        scr.nextLine();
                        val = scr.nextLine();
                        scr.close();
                        socket.close();
                        matrixCursor.addRow(new String[]{selection, val});
                        return matrixCursor;
                    }
                }catch (Exception ex) {
                    ex.printStackTrace();
                    List<Integer> replicationdata =   retIndex(actualIndex);
                    String msg = "SingleQuery:" + myPort + ":" + selection;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds_list.get(replicationdata.get(1)).myPort));
                    PrintWriter ptr = new PrintWriter(socket.getOutputStream(), true);
                    ptr.println(msg);
                    ptr.flush();
                    Scanner scr = new Scanner(socket.getInputStream());
                    scr.nextLine();
                    String val = scr.nextLine();
                    scr.close();
                    socket.close();
                    matrixCursor.addRow(new String[]{selection, val});
                    return matrixCursor;
                }
            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
