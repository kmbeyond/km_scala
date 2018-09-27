




---- IntelliJ setup----------------

--Add IntelliJ for Secured cluster (Kerberos)
#File>Settings
--Build, Execute, Deployment > Build Tools> SBT
---VM Parameters: -XX:MaxPermSize=384M
-Dhttp.proxyHost=webproxy.igslb.allstate.com
-Dhttp.proxyPort=8080
-Dhttps.proxyHost=webproxy.igslb.allstate.com
-Dhttps.proxyPort=8080
-Djava.security.auth.login.config=C:\km\kafka\kerberos\rtalab_vision.jaas
-Djava.security.krb5.conf=C:\km\kafka\kerberos\krb5.conf
---------


#Run/Debug Configurations (Edit Configurations..)
-Djava.security.auth.login.config=C:\km\kerberos\rtalab_vision\rtalab_vision.jaas -Djava.security.krb5.conf=C:\km\kerberos\krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false

