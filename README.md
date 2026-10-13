# Day 24 - Stateful vs Stateless Streaming

## Objective
Understand the difference between stateless and stateful processing using
Apache Spark Streaming and Scala.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.3
- Spark Streaming
- sbt 1.12.15
- Java 17

## Project Description
This project reads bank transaction records from a TCP socket and processes
them in 5-second batches.

Input format:
ACCOUNT_ID,TRANSACTION_ID

Example:
A101,T001
A101,T002
A102,T003

## Stateless Processing
Counts transactions for each account in the current batch only.
Previous batch counts are not retained.

## Stateful Processing
Uses updateStateByKey to maintain cumulative transaction counts for each
account across multiple batches. Checkpointing is enabled to support state
recovery.

## Run the Project
Start a TCP listener in one terminal:

    nc -lk 9998

Run the Spark application from another terminal:

    sbt "runMain StatefulStatelessStreaming"

Enter transaction records in the netcat terminal.

## Expected Behavior
If the first batch contains two transactions for A101 and one for A102:
- Stateless counts: A101 = 2, A102 = 1
- Stateful totals: A101 = 2, A102 = 1

If the next batch contains two transactions for A101 and one for A102:
- Stateless counts: A101 = 2, A102 = 1
- Stateful totals: A101 = 4, A102 = 2

## Key Learning
Stateless processing handles each batch independently, while stateful
processing maintains information across batches.
