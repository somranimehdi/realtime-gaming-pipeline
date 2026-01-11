#!/bin/bash

echo "Creating Elasticsearch mapping for game-events-time-agg..."

curl -X PUT "localhost:9200/game-events-time-agg" \
     -H 'Content-Type: application/json' \
     -d'
{
  "mappings": {
    "properties": {
      "event_window": { "type": "date" },
      "region": { "type": "keyword" },
      "event_type": { "type": "keyword" },
      "event_count": { "type": "long" },
      "doc_id": { "type": "keyword" }
    }
  }
}'

echo -e "\nMapping applied successfully."