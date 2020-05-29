package com.test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class PersonClassSerializer extends Serializer<Person> {
    @Override
    public void write(Kryo kryo, Output output, Person person) {

    }

    @Override
    public Person read(Kryo kryo, Input input, Class<Person> aClass) {
        return null;
    }
}
