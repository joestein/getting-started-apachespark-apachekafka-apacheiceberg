#!/bin/bash
set -e

python3 send_record.py --topic=my-topic --bootstrap-servers=localhost:9092 --schema-registry http://localhost:8081 --record-value='{"email": "email@email.com", "firstName": "Bob", "lastName": "Jones"}' --schema-file=create-user-request.avsc