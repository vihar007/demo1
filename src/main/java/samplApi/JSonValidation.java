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

static public boolean validate(String jsonData, String jsonSchema) {
    ProcessingReport report = null;
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

    return report.isSuccess();
}



}
