package com.snowflake.bharris.snowcamp;

import com.snowflake.snowpark_java.types.SnowflakeSecrets;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

public class FakeDataUDFV4 {

    public FakeDataUDFV4()
    {

    }

    public static void main(String[] args)
    {
        FakeDataUDFV4 udf = new FakeDataUDFV4();

        // This system.out.println doesn't really help us much.
        System.out.println(udf.callUrl(43.6532f, -79.3832f));
    }


    public static String callUrl(Float lat, Float lon) {
        try {

            SnowflakeSecrets sfSecrets = SnowflakeSecrets.newInstance();
            String apiKey = sfSecrets.getGenericSecretString("OPENWEATHERMAP_API_KEY");

            String url = "https://api.openweathermap.org/data/2.5/weather?lat=" + lat + "&lon=" + lon + "&exclude=hourly,daily&appid=" + apiKey;

            URL apiUrl = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                Scanner scanner = new Scanner(connection.getInputStream());
                StringBuilder response = new StringBuilder();
                while (scanner.hasNextLine()) {
                    response.append(scanner.nextLine());
                }
                scanner.close();
                return response.toString();
            } else {
                return "Error: " + responseCode;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return "Exception occurred: " + e.getMessage();
        }
    }


}
