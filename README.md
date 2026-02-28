**Project Overview**

This project implements a distributed message-processing microservice built with Java and Spring Boot. The service consumes streaming JSON messages from Kafka, applies classification and transformation logic, maintains version-controlled state using Redis, conditionally persists validated records to an external BLOB storage service, and routes results to RabbitMQ queues.

- Core functionality includes:
- Message classification and routing across queues
- Running aggregate tracking for valid records
- Conditional persistence with UUID enrichment
- Redis-based version control with tombstone resets
- Handling of out-of-order messages
- Strict timeout-bounded message consumption
- Containerized deployment using Docker
- The system simulates a real-world event-driven backend architecture where data flows across multiple distributed systems and must be processed reliably and        consistently.

Note: The files are not organized in their original folder structure due to storage limitations. The purpose of this repository is to provide representative code snippets demonstrating the core logic and concepts implemented. This project was completed as part of a Cloud Computing course.

