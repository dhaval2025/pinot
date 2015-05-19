package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.client.request.RequestConverter;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.pql.parsers.PQLCompiler;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.Collections;
import java.util.HashMap;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * TODO Document me!
 *
 * @author jfim
 */
public class Pql2CompilerTest {
  @Test
  public void testGeneratedQueries() {
    File avroFile = new File("pinot-integration-tests/src/test/resources/On_Time_On_Time_Performance_2014_1.avro");
    QueryGenerator qg = new QueryGenerator(Collections.singletonList(avroFile), "whatever", "whatever");

    PQLCompiler pql1Compiler = new PQLCompiler(new HashMap<String, String[]>());
    Pql2Compiler pql2Compiler = new Pql2Compiler();

    for (int i = 1; i <= 1000000; i++) {
      String pql = qg.generateQuery().generatePql();

      try {
        if (i % 1000 == 0) {
          System.out.print(".");
        }
        if (i % 100000 == 0) {
          System.out.println();
        }

        // Skip ones that don't compile with Pinot 1
        JSONObject jsonObject;
        try {
          jsonObject = pql1Compiler.compile(pql);
        } catch (Exception e) {
          continue;
        }

        BrokerRequest pqlBrokerRequest = RequestConverter.fromJSON(jsonObject);
        BrokerRequest pql2BrokerRequest = pql2Compiler.compileToBrokerRequest(pql);
        Assert.assertEquals(pqlBrokerRequest, pql2BrokerRequest);
      } catch (Exception e) {
        Assert.fail("Caught exception compiling " + pql, e);
      }
    }
  }

  @Test
  public void testLotsOfBqls() throws Exception {
    PQLCompiler pql1Compiler = new PQLCompiler(new HashMap<String, String[]>());
    Pql2Compiler pql2Compiler = new Pql2Compiler();
    LineNumberReader reader = new LineNumberReader(new FileReader("pinot-integration-tests/src/test/resources/lots-of-bqls.log"));

    int failedQueries = 0;

    String line = reader.readLine();
    while (line != null) {
      if (!line.contains("com.senseidb.ba")) {
        try {
          // Skip ones that don't compile with Pinot 1
          JSONObject jsonObject;
          try {
            jsonObject = pql1Compiler.compile(line);
          } catch (Exception e) {
            continue;
          }

          BrokerRequest pqlBrokerRequest = RequestConverter.fromJSON(jsonObject);
          BrokerRequest pql2BrokerRequest = pql2Compiler.compileToBrokerRequest(line);
          Assert.assertEquals(pqlBrokerRequest, pql2BrokerRequest);
        } catch (Exception e) {
          Assert.fail("failed query " + line, e);
        }
      }
      line = reader.readLine();
    }

    reader.close();

    Assert.assertEquals(failedQueries, 0, "Queries failed");
  }
}
