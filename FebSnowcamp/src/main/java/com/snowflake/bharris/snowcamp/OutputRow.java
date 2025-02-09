package com.snowflake.bharris.snowcamp;

public class OutputRow
{
    public String firstName;
    public String lastName;
    public String emailAddress;
    public String phoneNumber;


    public OutputRow(String firstName, String lastName, String emailAddress, String phoneNumber) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.emailAddress = emailAddress;
        this.phoneNumber = phoneNumber;
    }
}
