---- IntelliJ setup----------------

--Add IntelliJ for Secured cluster (Kerberos)
#File>Settings
--Build, Execute, Deployment > Build Tools> SBT
---VM Parameters: -XX:MaxPermSize=384M
-Dhttp.proxyHost=webproxy.abc.com
-Dhttp.proxyPort=8080
-Dhttps.proxyHost=webproxy.abc.com
-Dhttps.proxyPort=8080
-Djava.security.auth.login.config=C:\km\kerberos\rt.jaas
-Djava.security.krb5.conf=C:\km\kerberos\krb5.conf
---------


#Run/Debug Configurations (Edit Configurations..)
-Djava.security.auth.login.config=C:\km\kerberos\rt.jaas -Djava.security.krb5.conf=C:\km\kerberos\krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false
