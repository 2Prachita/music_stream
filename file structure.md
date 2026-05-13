music_stream_pipeline/ 
├── 📄 docker-compose.yml ← Kafka + Zookeeper (copy-paste, we'll build this) 
├── 📄 requirements.txt 
├── 📄 Makefile 
├── 📄 README.md 
├── 📄 .env.example 
├── 📄 .gitignore 
├── 📄 set_env.sh.example 
│ ├── 📁 producer/ 
│ ├── event_schema.py ← dataclass defining PlayEvent shape 
│ ├── event_generator.py ← Faker: generates realistic events 
│ └── kafka_producer.py ← publishes to Kafka topics 
│ ├── 📁 consumer/ 
│ ├── raw_consumer.py ← Python: lands raw events → Snowflake RAW 
│ └── spark_streaming.py ← PySpark: micro-batch aggregations 
│ ├── 📁 snowflake/ 
│ └── setup.sql ← creates schemas + tables (run once) 
│ ├── 📁 dbt_project/ 
│ ├── dbt_project.yml 
│ ├── profiles.yml ← copy from Project 1 
│ ├── packages.yml 
│ ├── macros/ 
│ │ └── generate_schema_name.sql ← copy from Project 1 
│ ├── models/ 
│ │ ├── staging/
│ │ │ ├── sources.yml 
│ │ │ ├── stg_play_events.sql 
│ │ │ └── stg_agg_plays.sql 
│ │ └── marts/ 
│ │ ├── fct_play_events.sql 
│ │ └── mart_listening_trends.sql 
│ └── tests/ 
│ └── generic_tests.yml 
│ └── 📁 utils/ 
└── logger.py ← copy from Project 1