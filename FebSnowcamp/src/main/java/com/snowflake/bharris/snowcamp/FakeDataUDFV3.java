package com.snowflake.bharris.snowcamp;

import com.github.javafaker.Faker;

import java.util.ArrayList;
import java.util.List;
import Sno

public class FakeDataUDFV3 {

    public FakeDataUDFV3()
    {

    }

    public static void main(String[] args)
    {
        // Snowflake won't call this
        // It's only here for testing locally
        FakeDataUDFV3 udf = new FakeDataUDFV3();

        // This system.out.println doesn't really help us much.
        System.out.println(udf.process(10));
    }

    public static Class getOutputClass() {
        return OutputRow.class;
    }




    public Stream<OutputRow> process(int num) {
        List<OutputRow> rows = new ArrayList<>();
        for (int x = 0; x < num; x++) {
            rows.add(createBasicRows());
        }

        return Stream.of(rows.stream().toArray(OutputRow[]::new));

    }

    private static OutputRow createBasicRows() {
        Faker faker = new Faker();

        String fName = faker.name().firstName();
        String lName = faker.name().lastName();

        OutputRow row = new OutputRow(
                fName,
                lName,
                fName + "." + lName + "@" + faker.internet().domainName(),
                faker.phoneNumber().cellPhone());
        return row;
    }
}
