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
	// public static String myPort;
	public static String myUri = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	String[] portlist = new String[]{"11108", "11112", "11116", "11120","11124"};
	private static String myPort;
	private static  String myNode;
	private static boolean  starQueryResult = false;


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

	private ArrayList<MyAvd> avds_list = new ArrayList  <MyAvd>();
	public static MyAvd prev, curr;
	public int cur, succ1, succ2;

    public class Compare implements Comparator<String> {

        @Override
        public int compare(String lhs, String rhs) {

            String left = "", right = "";
            try {
//                left = genHash(String.valueOf(Integer.parseInt(lhs)/2));
                left = genHash(lhs);
//                right = genHash(String.valueOf(Integer.parseInt(rhs)/2));
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
        } catch (Exception ex) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        return false;


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
                    }
                    else if( msg[0].equals("Query"))
                    {
                        String columnNames[] = new String[]{"key", "value"};
                        Log.e("At server - " + myPort ," REquest for query from-" + msg[1]);
                        MatrixCursor matrixCursor = new MatrixCursor( new String [] {"key","value"});
                        for (File file : getContext().getFilesDir().listFiles()) {
                            DataInputStream dis = new DataInputStream(getContext().openFileInput(file.getName()));
                            String value = dis.readUTF();
                            columnNames = new String[]{file.getName(), value};
                            matrixCursor.addRow(columnNames);
                        }



                    }
                }

            } catch (IOException e) {
                Log.e(TAG, "I/O Exception");
            } /*catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }*/
            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {

            String temp = strings[0];
            String[] msg = temp.split(":");
            try{
                String port = msg[3];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                PrintWriter ptr = new PrintWriter(socket.getOutputStream(), true);
                ptr.println(temp);
                ptr.flush();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }






    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
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
            for(int i =0 ; i < temp_list.size() ; i++)
            {
                if( hashedKey.compareTo(genHash(temp_list.get(i))) <0 )
                {
                    ind = i;
                }
            }
            if( ind == -1) {
                ind =0;
            }
            List<Integer> indexes = retIndex(ind);
            Log.e("Index length-" , indexes.size() + "");
            Log.e("Key index - " + ind , " And it should be inserted in " + indexes.get(0).toString() + "-" + indexes.get(1) + " -" + indexes.get(2));
            for (int index : indexes) {

                if(temp_list.get(ind).equals(myNode))
                {
                    Log.e("Insert in my node-", "Key-" + key);
                    Log.e("Insert in avd with","key = "+  genHash(key));
                    DataOutputStream dos = new DataOutputStream(getContext().openFileOutput(key, Context.MODE_PRIVATE));
                    dos.writeUTF(value);
                }
                String message = "InsertKey" + ":" + key + ":" + value + ":" + String.valueOf(Integer.parseInt(avds_list.get(index).myPort) );
                Log.e("Message to send", message);
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
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
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
            else if (selection.equals("*"))
            {
                File files[] = getContext().getFilesDir().listFiles();
                for (File file : files) {
                    DataInputStream dis = new DataInputStream(getContext().openFileInput(file.getName()));
                    String value = dis.readUTF();
                    columnNames = new String[]{file.getName(), value};
                    matrixCursor.addRow(columnNames);
                }
               for(int i =0 ; i< avds_list.size();i++)
               {
                   if( !avds_list.get(i).equals(myPort)) {
                       String message = "Query" + ":" + myPort + ":" + "NA" + ":" + String.valueOf(Integer.parseInt(avds_list.get(i).myPort));
                       Log.e("Message to send", message);
                       new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                       Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds_list.get(i).myPort));
                       DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                       outputStream.writeUTF(message);
                       outputStream.flush();

                       DataInputStream dis = new DataInputStream(socket.getInputStream());
                       String receivedMessage = dis.readUTF();
                       String []receivedMessageTokens = receivedMessage.split(":");
                      // Scanner scr = new Scanner(socket.getInputStream());
                       String keyValue = receivedMessageTokens[1];
                       String []keyValuePairs = keyValue.split("##");
                       for( String keyAndValue : keyValuePairs)
                       {
                           String []keyAndValueTokens = keyAndValue.split("-");
                           matrixCursor.addRow(new String [] {keyAndValueTokens[0] , keyAndValueTokens[1]});

                       }



                   }
               }

               return matrixCursor;



            }


        }
        catch(Exception ex)
        {
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

    public static Uri getUri() {
        Uri.Builder builder = new Uri.Builder();
        builder.authority(myUri);
        builder.scheme("content");
        Uri uri = builder.build();
        return uri;
    }
}