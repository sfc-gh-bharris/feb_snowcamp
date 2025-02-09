package com.snowflake.bharris.snowcamp;

import com.github.javafaker.Faker;

public class FakeDataUDFV2 {
    public static void main(String[] args)
    {
        // Snowflake won't call this
        // It's only here for testing locally
        FakeDataUDFV2 udf = new FakeDataUDFV2();
        System.out.println(udf.sayHello(udf.createAName(false)));
    }

    private String sayHello(String name) {
        return "Hello: " + name;
    }

    private String createAName(Boolean full)
    {
        Faker faker = new Faker();
        if(full)
        {
            return faker.name().firstName() + " " + faker.name().lastName();
        }
        return faker.name().firstName();
    }
}
