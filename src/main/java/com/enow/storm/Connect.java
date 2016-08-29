package com.enow.storm;

import java.net.URL;
import org.json.simple.JSONObject;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URLConnection;

public class Connect {
    private URL url = null;

    public Connect(){
    }

    public Connect(String in_url){
        try {
            url = new URL(in_url);
        }catch(MalformedURLException e){
            e.printStackTrace();
        }
    }

    public URL getURL(){
        return url;
    }

    public static String post(URL url, JSONObject json){
        DataOutputStream printout;
        try {
            URLConnection urlConn = url.openConnection();
            urlConn.setDoInput(true);
            urlConn.setDoOutput(true);
            urlConn.setUseCaches(false);
            urlConn.setRequestProperty("Content-Type", "application/json");
            urlConn.connect();

            printout = new DataOutputStream(urlConn.getOutputStream ());
            String str = json.toString();
            byte[] data=str.getBytes("UTF-8");
            printout.write(data);
            printout.flush();
            printout.close();

            HttpURLConnection c = (HttpURLConnection) urlConn;
            int status = c.getResponseCode();

            switch (status) {
                case 200:
                case 201:
                    BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream()));
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = br.readLine()) != null) {
                        sb.append(line + "\n");
                    }
                    br.close();

                    return sb.toString();
            }
        }catch (MalformedURLException e){
            e.printStackTrace();
            return null;
        }catch (IOException e){
            e.printStackTrace();
            return null;
        }
        return null;
    }
}