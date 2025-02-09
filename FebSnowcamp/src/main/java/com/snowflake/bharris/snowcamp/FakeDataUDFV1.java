package com.snowflake.bharris.snowcamp;

public class FakeDataUDFV1 {
    public static void main(String[] args)
    {
        // Snowflake won't call this
        // It's only here for testing locally
        FakeDataUDFV1 udf = new FakeDataUDFV1();
        System.out.println(udf.sayHello("Brad"));
    }

    private String sayHello(String name) {
        return "Hello: " + name;
    }
}
