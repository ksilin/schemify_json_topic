
from confluent_kafka import Consumer
from genson import SchemaBuilder
import json
from collections import defaultdict
import time
import argparse

def evaluate_message(msg, field_types, field_counts, field_examples):
    data = json.loads(msg)
    for key, value in data.items():
        field_counts[key] += 1
        field_types[key].add(type(value).__name__)
        field_examples[key] = value

def print_summary(total_messages, field_counts, field_types, field_examples):
    print(f"\nTotal messages processed: {total_messages}")
    mandatory_fields = {key for key, count in field_counts.items() if count == total_messages}
    potential_issues = {key: types for key, types in field_types.items() if len(types) > 1}
    print(f"Field Occurrences: {dict(field_counts)}")
    print(f"Field Types: {dict(field_types)}")
    print(f"Mandatory Fields: {mandatory_fields}")
    print(f"Potential Issues: {potential_issues}")
    print(f"Example message: {field_examples}")
    builder = SchemaBuilder()
    builder.add_object(field_examples)
    schema = builder.to_schema()
    schema['required'] = [*mandatory_fields,]
    print("derived schema:")
    print(json.dumps(schema, indent=4))

def main(config_file, topic):
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'schemify_group',
        'auto.offset.reset': 'earliest'
    }
    if config_file:
        with open(config_file, 'r') as file:
            conf.update(json.load(file))
    if args.group is not None:
        conf['group.id'] = args.group

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    field_types = defaultdict(set)
    field_counts = defaultdict(int)
    field_examples = dict()
    total_messages = 0
    start_time = time.time()

    try:
        while True:

            if args.max_idle is not None and time.time() - start_time > args.max_idle:
                break
            if args.max_msg is not None and total_messages >= args.max_msg:
                break

            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            evaluate_message(msg.value(), field_types, field_counts, field_examples)
            start_time = time.time()
            total_messages += 1

            if total_messages % 100 == 0:
                print(f"\rProcessed {total_messages} messages so far.", end='')

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print_summary(total_messages, field_counts, field_types, field_examples)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='aggregate all JSON message fields in a topic')
    parser.add_argument('--config', type=str, help='path to configuration file')
    parser.add_argument('--topic', type=str, help='the topic to analyze')
    parser.add_argument('--group', type=str, help='the consumer group to use')
    parser.add_argument('--max_msg', type=int, default=None, help='abort analysis after this many messages')
    parser.add_argument('--max_idle', type=int, default=None, help='abort analysis if next message does not arrive in this many seconds')
    args = parser.parse_args()
    main(args.config, args.topic)
