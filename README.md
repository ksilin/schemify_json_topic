## infer JSON schema from a raw JSON topic

### produce test data

`python jsongen.py --total_fields 10 --mandatory_fields 2 --type_issues 0 --total_messages 100 | kcat -b pkc-***.confluent.cloud:9092 -L -F ./ccloud.***.kcat.props -t json_raw_temp -P`

### verify test data

`kcat -b pkc-***.confluent.cloud:9092 -L -F ./ccloud.***.kcat.props -t json_raw_temp -C`

### derive schema

`python schemify_topic.py --config ./ccloud.mvzwd2.props.json --topic json_raw_temp --max_msg 1000 --max-idle 3 --group mygroup`

