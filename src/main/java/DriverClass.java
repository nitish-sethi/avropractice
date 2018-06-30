import my.pkg.path.avro.PeopleList;
import my.pkg.path.avro.Person;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DriverClass {
    public static void main(String[] args) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder e = EncoderFactory.get().binaryEncoder(out,null);
        SpecificDatumWriter<PeopleList> w =
                new SpecificDatumWriter<PeopleList>(PeopleList.class);

        PeopleList all = new PeopleList();
        all.People = new GenericData.Array<Person>(
                3, all.getSchema().getField("People").schema());

        Person person1 = new Person();
        person1.FirstName = new Utf8("Cairne");
        person1.LastName = new Utf8("Bloodhoof");
        all.People.add(person1);

        Person person2 = new Person();
        person2.FirstName = new Utf8("Sylvanas");
        person2.LastName = new Utf8("Windrunner");
        all.People.add(person2);

        Person person3 = new Person();
        person3.FirstName = new Utf8("Grom");
        person3.LastName = new Utf8("Hellscream");
        all.People.add(person3);

        all.Version = 1;
        w.write(all, e);
        e.flush();
        ByteBuffer serialized = ByteBuffer.allocate(out.toByteArray().length);
        serialized.put(out.toByteArray());
        System.out.println("Done!!");
        System.out.println( serialized.array());

    }
}
