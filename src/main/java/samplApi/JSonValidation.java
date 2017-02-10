package samplApi;

import java.io.IOException;
import java.util.Iterator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.github.fge.jsonschema.report.ProcessingMessage;
import com.github.fge.jsonschema.report.ProcessingReport;
import com.github.fge.jsonschema.util.JsonLoader;


public class JSonValidation  
{

public boolean validate(String jsonData, String jsonSchema) {
    ProcessingReport report = null;
    boolean result = false;
    try {
        
        JsonNode schemaNode = JsonLoader.fromString(jsonSchema);
        JsonNode data = JsonLoader.fromString(jsonData);         
        JsonSchemaFactory factory = JsonSchemaFactory.byDefault(); 
        JsonSchema schema = factory.getJsonSchema(schemaNode);
        report = schema.validate(data);
    } catch (JsonParseException jpex) {
        jpex.printStackTrace();
    } catch (ProcessingException pex) {  
        pex.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }
//    if (report != null) {
//        Iterator<ProcessingMessage> iter = report.iterator();
//        while (iter.hasNext()) {
//            ProcessingMessage pm = iter.next();
//            System.out.println("Processing Message: "+pm.getMessage());
//        }
//        result = report.isSuccess();
//    }
//    System.out.println(" Result=" +result);
    return report.isSuccess();
}



}
