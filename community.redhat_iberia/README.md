# community.redhat_iberia Ansible plugins

This folder contains a set of Ansible plugins (filter plugins and modules) to work with Avro data and Kafka.

The README below summarizes the available filters and modules, their purpose, requirements, and short examples extracted from the plugin documentation.

## Contents
- Filters
  - `to_avro` — convert Python objects to Avro binary
  - `message_from_avro` — decode Avro binary to Python objects
  - `schema_id_from_avro` — extract a schema id embedded in Avro binary (helper)
- Modules
  - `avro_generate` — generate sample data from an Avro schema (requires `fastavro`)
  - `avro_validate` — validate data against an Avro schema (requires `fastavro`)
  - `kafka_send` — send messages to Kafka topics (requires `kafka-python`)

## Filters

All Avro filters use the `fastavro` library. The filters are implemented in `plugins/filter/avro.py` and include:

- to_avro
  - Purpose: Encode a Python object (dict, list, etc.) into Avro binary using a provided schema.
  - Key args: `obj` (object), `schema` (dict or JSON string), `schema_id` (optional), `schemaless` (bool).
  - Example (Jinja/Ansible):

    ```yaml
    - set_fact:
        avro_binary: "{{ user_data | to_avro(user_schema) }}"
    ```

- message_from_avro
  - Purpose: Decode Avro binary back to a Python object using a provided schema.
  - Key args: `avro_bytes`, `schema`, `schema_id` (optional), `schemaless` (bool).
  - Example:

    ```yaml
    - set_fact:
        decoded_user: "{{ avro_binary | message_from_avro(user_schema) }}"
    ```

- schema_id_from_avro
  - Purpose: Extract an embedded schema id from Avro binary that uses a 1-byte magic + 4-byte id header.
  - Note: The implementation reads the magic byte and the following 4 bytes as the schema id.

Files providing documentation for the filters (examples and short descriptions):
- `plugins/filter/to_avro.yml`
- `plugins/filter/message_from_avro.yml`
- `plugins/filter/schema_id_from_avro.yml` (empty file present but functionality implemented in `avro.py`)

## Modules

1) avro_generate
   - Location: `plugins/modules/avro_generate.py`
   - Purpose: Generate one or more sample records that conform to a provided Avro schema.
   - Requirements: `fastavro`
   - Arguments:
     - `schema` (required): Avro schema as a dict or JSON string
     - `num_samples` (optional, default 1): number of samples to generate
   - Returns: `samples` (list)
   - Example:

     ```yaml
     - name: Generate a sample user record
      # community.redhat_iberia collection — Avro + Kafka plugins

      This collection provides Ansible plugins (filters and modules) to work with Avro-encoded data and Apache Kafka. It includes:

      - Filter plugins: `to_avro`, `message_from_avro`, `schema_id_from_avro`
      - Modules: `avro_generate`, `avro_validate`, `kafka_send`

      For full module/plugin options and examples please consult the `DOCUMENTATION` and `EXAMPLES` strings at the top of each module/filter implementation (files under `plugins/`). This README provides an overview tailored for Ansible Galaxy consumers.

      ## Install

      Install requirements used by the modules/filters in your execution environment (system or virtualenv):

      ```bash
      pip install fastavro kafka-python
      ```

      Install the collection from Ansible Galaxy (once published) or install locally from this repository:

      ```bash
      # From Galaxy (example):
      ansible-galaxy collection install community.redhat_iberia

      # From local repo:
      ansible-galaxy collection build && ansible-galaxy collection install ./community-redhat_iberia-*.tar.gz
      ```

      ## Supported content

      - Filters (implemented in `plugins/filter/avro.py`)
        - `to_avro`: encode Python data to Avro binary using a schema
        - `message_from_avro`: decode Avro binary to Python objects using a schema
        - `schema_id_from_avro`: extract schema id embedded in Avro payloads using the Confluent wire format (1-byte magic + 4-byte id)

      - Modules (in `plugins/modules`)
        - `avro_generate`: generate sample records that conform to a provided Avro schema (requires `fastavro`)
        - `avro_validate`: validate data against an Avro schema (requires `fastavro`)
        - `kafka_send`: send messages to Kafka topics (requires `kafka-python`)

      See the top of each module file for the complete `DOCUMENTATION` and `EXAMPLES` blocks which are used by Ansible to present help in `ansible-doc`.

      ## Quick examples

      Generate a sample Avro record:

      ```yaml
      - name: Generate a sample record
        avro_generate:
          schema:
            type: record
            name: User
            fields:
              - {name: name, type: string}
              - {name: age, type: int}
        register: sample

      # `sample.samples` contains a list of records
      ```

      Validate data against an Avro schema:

      ```yaml
      - name: Validate user data
        avro_validate:
          schema: "{{ user_schema }}"
          data: "{{ user_data }}"
        register: validation

      - debug: var=validation.valid
      ```

      Send a text message to Kafka:

      ```yaml
      - name: Send text message to Kafka
        kafka_send:
          topic: my_topic
          message: "Hello, Kafka!"
          bootstrap_servers: localhost:9092
      ```

      Send Avro payload produced by the `to_avro` filter:

      ```yaml
      - set_fact:
          avro_payload: "{{ my_obj | to_avro(my_schema) }}"

      - name: Send Avro payload
        kafka_send:
          topic: avro_topic
          message: "{{ avro_payload }}"
          bootstrap_servers: kafka:9092
      ```

      ## Requirements

      - Python packages (install via pip): `fastavro`, `kafka-python`
      - Ansible: see `community.redhat_iberia/meta/runtime.yml` for minimum Ansible requirement

      ## Contributing

      If you contribute code, update the `DOCUMENTATION` and `EXAMPLES` variables in the corresponding plugin/module file and add/update tests under the `tests/` directory. Use the standard repository contribution flow and open issues/PRs in this GitHub repository:

      https://github.com/redhat-iberia/ansible-kafka-module

      ## License

      This repository includes a top-level `LICENSE` file. The collection metadata includes the license information used when publishing to Galaxy.

      ## Support & Issues

      Report issues and feature requests in the GitHub repo:

      https://github.com/redhat-iberia/ansible-kafka-module/issues
