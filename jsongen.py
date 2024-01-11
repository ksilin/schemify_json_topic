import json
import random
import argparse


def generate_value(value_type):
    if value_type == str:
        return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=5))
    elif value_type == int:
        return random.randint(1, 100)
    elif value_type == float:
        return round(random.uniform(1.0, 100.0), 2)
    elif value_type == bool:
        return random.choice([True, False])
    else:
        return None

def generate_json_data(total_fields, mandatory_fields, type_issues, total_messages):

    field_names = [f"field_{i}" for i in range(total_fields)]
    mandatory_field_names = random.sample(field_names, mandatory_fields)
    types = [str, int, float, bool]

    for _ in range(total_messages):
        message = {}

        for field in mandatory_field_names:
            message[field] = generate_value(random.choice(types))

        optional_fields = set(field_names) - set(mandatory_field_names)
        for field in optional_fields:
            if random.choice([True, False]):
                if field in field_names[:type_issues]:
                    message[field] = generate_value(random.choice(types))
                else:
                    message[field] = generate_value(str)

        print(json.dumps(message))

def main():
    parser = argparse.ArgumentParser(description='generate JSON data for topic aggregation testing.')
    parser.add_argument('--total_fields', type=int, default=10, help='total number of fields in the JSON msgs')
    parser.add_argument('--mandatory_fields', type=int, default=2, help='number of mandatory fields')
    parser.add_argument('--type_issues', type=int, default=0, help='number of fields with more than one type')
    parser.add_argument('--total_messages', type=int, default=100, help='total number of messages to generate')

    args = parser.parse_args()

    generate_json_data(args.total_fields, args.mandatory_fields, args.type_issues, args.total_messages)

if __name__ == "__main__":
    main()
