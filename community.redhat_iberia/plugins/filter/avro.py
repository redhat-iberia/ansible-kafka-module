from ansible.errors import AnsibleFilterError
import fastavro
import io
import json

def to_avro(obj, schema, schema_id=None, schemaless=True):
  """
  Convert a Python object to Avro binary format using the provided schema.
  
  Args:
    obj: The Python object to convert
    schema: Avro schema definition as a dictionary
    schema_id: Optional schema ID to include
    schemaless: Whether to use schemaless writer (default: True)

  Returns:
    bytes: Avro binary encoded data
  
  Raises:
    AnsibleFilterError: If there's an error during conversion
  """
  try:
    # Convert string inputs to dict if needed
    if isinstance(schema, str):
        try:
            schema = json.loads(schema)
        except json.JSONDecodeError as e:
            raise AnsibleFilterError(f'Invalid JSON in schema: {str(e)}')

    if isinstance(obj, str):
        try:
            obj = json.loads(obj)
        except json.JSONDecodeError as e:
            raise AnsibleFilterError(f'Invalid JSON in data: {str(e)}')

    # Parse the Avro schema
    schema = fastavro.parse_schema(schema)
    
    # Create a binary stream to write the data
    bytes_writer = io.BytesIO()
    
    if schema_id is not None:
        # Prepend schema ID to the binary data if provided
        bytes_writer.write(b'\x00')  # Magic byte
        bytes_writer.write(schema_id.to_bytes(4, byteorder='big'))

    # Write the object using fastavro
    if schemaless:
        fastavro.schemaless_writer(bytes_writer, schema, obj)
    else:
        fastavro.writer(bytes_writer, schema, [obj])
    
    # Get the binary data
    avro_bytes = bytes_writer.getvalue()
    return avro_bytes
    
  except Exception as e:
    raise AnsibleFilterError(f'Error converting to Avro: {str(e)}')

def schema_id_from_avro(avro_bytes):
  """
  Extract the Avro schema id from Avro binary data. Only works for schemaless Avro data.
  
  Args:
    avro_bytes: The Avro binary data

  Returns:
    string: Avro schema id

  Raises:
    AnsibleFilterError: If there's an error during extraction
  """

  try:
    # Convert input to bytes if it's a string or AnsibleUnsafeText
    if not isinstance(avro_bytes, bytes):
        avro_bytes = str(avro_bytes).encode('utf-8')

    # Create a binary stream with the input data
    bytes_reader = io.BytesIO(avro_bytes)
    
    magic_byte = bytes_reader.read(1)
    if magic_byte != b'\x00':
        raise AnsibleFilterError('Invalid magic byte in Avro data')
    schema_id = int.from_bytes(bytes_reader.read(4), byteorder='big')
    return str(schema_id)
    
  except Exception as e:
    raise AnsibleFilterError(f'Error extracting schema id from Avro: {str(e)}')

def message_from_avro(avro_bytes, schema, schema_id=None, schemaless=True):
  """
  Convert Avro binary data back to a Python object using the provided schema.
  
  Args:
    avro_bytes: The Avro binary data to convert
    schema: Avro schema definition as a dictionary
    schema_id: Optional schema ID to validate
    schemaless: Whether to use schemaless reader (default: True)
  
  Returns:
    object: Python object decoded from Avro
  
  Raises:
    AnsibleFilterError: If there's an error during conversion
  """
  try:
    # Convert input to bytes if it's a string or AnsibleUnsafeText
    if not isinstance(avro_bytes, bytes):
        avro_bytes = str(avro_bytes).encode('utf-8')

    # Convert string inputs to dict if needed
    if isinstance(schema, str):
        try:
            schema = json.loads(schema)
        except json.JSONDecodeError as e:
            raise AnsibleFilterError(f'Invalid JSON in schema: {str(e)}')
        
    # Parse the Avro schema
    schema = fastavro.parse_schema(schema)
    
    # Create a binary stream with the input data
    bytes_reader = io.BytesIO(avro_bytes)
    
    if schema_id is not None:
        # Read and validate schema ID
        magic_byte = bytes_reader.read(1)
        if magic_byte != b'\x00':
            raise AnsibleFilterError('Invalid magic byte in Avro data')
        read_schema_id = int.from_bytes(bytes_reader.read(4), byteorder='big')
        if str(read_schema_id) != schema_id:
            raise AnsibleFilterError(f'Schema ID mismatch: expected {schema_id}, got {read_schema_id}')

    # Read the object using fastavro
    if schemaless:
        record = fastavro.schemaless_reader(bytes_reader, schema, None)
    else:
        iterator = fastavro.reader(bytes_reader, schema)
        record = next(iterator)

    return record
    
  except Exception as e:
    raise AnsibleFilterError(f'Error converting from Avro: {str(e)}')

class FilterModule(object):
  """Ansible filters for Avro operations"""

  def filters(self):
    return {
      'to_avro': to_avro,
      'schema_id_from_avro': schema_id_from_avro,
      'message_from_avro': message_from_avro
    }