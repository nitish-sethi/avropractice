import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class DriverClassGeneric {
    public static void main(String[] args) throws IOException {
        Schema schema = Schema.parse(new File("Schema.avsc"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        //for the binary encoded avro we use the format below
        //Encoder e = EncoderFactory.get().binaryEncoder(out,null);

        //and for the json encoded avro we use the format below
        Encoder e = EncoderFactory.get().jsonEncoder(schema,out);

        GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(schema);
        GenericRecord all = new GenericData.Record(schema);
        Schema peopleSchema = schema.getField("People").schema();
        GenericArray<GenericRecord> people = new GenericData.Array<GenericRecord>(3, peopleSchema);
        Schema personSchema = peopleSchema.getElementType();

        GenericRecord person1 = new GenericData.Record(personSchema);
        person1.put("FirstName", new Utf8("Cairne"));
        person1.put("LastName", new Utf8("Bloodhoof"));
        people.add(person1);

        GenericRecord person2 = new GenericData.Record(personSchema);
        person2.put("FirstName", new Utf8("Sylvanas"));
        person2.put("LastName", new Utf8("Windrunner"));
        people.add(person2);

        GenericRecord person3 = new GenericData.Record(personSchema);
        person3.put("FirstName", new Utf8("Grom"));
        person3.put("LastName", new Utf8("Hellscream"));
        people.add(person3);

        all.put("People", people);
        all.put("Version", 1);
        w.write(all, e);
        e.flush();
        ByteBuffer serialized = ByteBuffer.allocate(out.toByteArray().length);
        serialized.put(out.toByteArray());
        System.out.println("Execution completed.");
        String v = new String( serialized.array(), Charset.forName("UTF-8") );
        System.out.println(v);
    }
}
