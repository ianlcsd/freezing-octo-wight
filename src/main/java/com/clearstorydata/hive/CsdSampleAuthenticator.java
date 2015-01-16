package com.clearstorydata.hive;

import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;
import java.util.Hashtable;

/*
1. cd /tmp
2. javac -cp  /Users/Ian1/.m2/repository/org/apache/hive/hive-service/0.14.0/hive-service-0.14.0.jar /Users/Ian1/workspace/hive/poc/hive-agent-poc/src/main/java/com/clearstorydata/hive/CsdSampleAuthenticator.java -d .
3.  jar cf simpleauth.jar com
4. scp simpleauth.jar ec2-user@10.1.1.22:~/
5. ssh to ec2-user@10.1.1.22  and issue the following in command prompt
       sudo cp ~/simpleauth.jar  /usr/hdp/2.2.0.0-2041/hive/lib/.'
6. in ambari’s console, http://10.1.1.22:8080/#/main/hosts/localhost.localdomain/configs
 define a new CSDSIMPLEAUTH with the following added/overriding properties

<property>
  <name>hive.server2.authentication</name>
  <value>CUSTOM</value>
</property>

<property>
  <name>hive.server2.custom.authentication.class</name>
  <value>org.apache.hive.service.auth.PasswdAuthenticationProvider.SampleAuth</value>
</property>

7. bounce the cluster using Ambari
*/


public class CsdSampleAuthenticator implements PasswdAuthenticationProvider {

    Hashtable<String, String> store = null;

    public CsdSampleAuthenticator () {
        store = new Hashtable<String, String>();
        store.put("hive", "hive");
        store.put("foobar", "Barf0000");

    }

    @Override
    public void Authenticate(String user, String  password)
            throws AuthenticationException {

        String storedPasswd = store.get(user);
        if (user.equalsIgnoreCase("anonymous")) {
            return;
        } else if (storedPasswd != null && storedPasswd.equals(password))
            return;

        throw new AuthenticationException("SampleAuthenticator: Error authenticating user:" + user );
    }

}