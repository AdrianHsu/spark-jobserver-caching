#!/bin/bash

curl -X POST localhost:8090/binaries/test -H "Content-Type: application/java-archive" --data-binary @job-server-tests/target/scala-2.11/job-server-tests_2.11-0.10.1-SNAPSHOT.jar

curl -d "" "localhost:8090/contexts/test-context"

curl -d "input.string = y a b c a b see m i k j test run level z x c v b n m is" "localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountExample&context=test-context&sync=true"

