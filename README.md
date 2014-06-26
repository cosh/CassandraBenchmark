Startup
-------
mvn spring-boot:run



Datamodel
---------

curl -H "Content-Type: application/json" -d '{"benchmarkName":"datastaxBatchInsert", "seedNode":"internalIPOfSeedNodeInSameZone"}' http://127.0.0.1:8080/scenario/createDatamodel

curl -H "Content-Type: application/json" -d '{"benchmarkName":"astyanaxBatchInsert", "seedNode":"internalIPOfSeedNodeInSameZone"}' http://127.0.0.1:8080/scenario/createDatamodel


Execution
---------

curl -H "Content-Type: application/json" -d '{"benchmarkName":"datastaxBatchInsert", "seedNode":"internalIPOfSeedNodeInSameZone", "parameter": {"batchSize":"100", "rowCount": "100000"}}' http://127.0.0.1:8080/scenario/execute

curl -H "Content-Type: application/json" -d '{"benchmarkName":"astyanaxBatchInsert", "seedNode":"internalIPOfSeedNodeInSameZone", "parameter": {"batchSize":"100", "rowCount": "100000"}}' http://127.0.0.1:8080/scenario/execute
