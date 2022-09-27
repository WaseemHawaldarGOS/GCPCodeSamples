Prerequisites to exeuct this code in local:
1. spanner emulator running in local using docker image
2. pubsub emulator(pubsub fake server) running in local using gcloud beta
3. python client setup in local to publish messages to pubsub emulator
4. have gcloud account

Steps to create employee table:
step1 : Install intellij enterprise edition (could be 1 month trial)
step2: Go to database navigator, click on '+' sign to create new DB, select spanner DB
install all the drivers.
step3: enter below details to connect to spanner-emulator
project - total-method-360201
instance - local-instance
db - localdb
step 4: execute below ddl command to create employee table

Steps to execute this code:
Right click on Startup.java and execute with below params:
Program args:
--pubSubSubscription=projects/total-method-360201/subscriptions/subsid
--pubSubHost=http://localhost:8085
--spannerHost=http://localhost:9010
--instanceID=local-instance
--databaseID=localdb
--project=total-method-360201

environment variables:
SPANNER_EMULATOR_HOST=localhost:9010

