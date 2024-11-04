import socket  # noqa: F401
import os
import sys
# import process_log
API_VERSIONS = 18
FETCH = 1
DESCRIBE_TOPIC_PARTITIONS = 75
NO_ERROR = 0
UNKNOWN_TOPIC_OR_PARTITION = 3
MSB_SET_MASK = 0b10000000
REMOVE_MSB_MASK = 0b01111111
SIGNED_VARINT = 0
UNSIGNED_VARINT = -1
LENGTH_PREVIOUS = -10
LENGTH_PREVIOUS_UVARINT = -11
LENGTH_PREVIOUS_SVARINT = -12
batch_fields = [
    ("base_offset", 8),
    ("batch_length", 4),
    ("partition_leader_epoch", 4),
    ("magic_byte", 1),
    ("crc", 4),
    ("attributes", 2),
    ("last_offset_delta", 4),
    ("base_timestamp", 8),
    ("max_timestamp", 8),
    ("producer_id", 8),
    ("producer_epoch", 2),
    ("base_sequence", 4),
    ("records_length", 4),
]
# Field length SIGNED_VARINT => signed VARINT ZigZag
record_fields = [
    ("length", SIGNED_VARINT),
    ("attributes", 1),
    ("timestamp_delta", SIGNED_VARINT),
    ("offset_delta", SIGNED_VARINT),
    ("key_length", SIGNED_VARINT),
    ("value_length", SIGNED_VARINT),
    ("frame_version", 1),
    ("type", 1),
]
specific_fields = {
    b"\x0c": [  # Feature Level record
        ("version", 1),
        ("name_length", UNSIGNED_VARINT),
        ("name", LENGTH_PREVIOUS_UVARINT),
        ("feature_level", 2),
        ("tagged_fields_count", 1),
        ("headers_array_count", 1),
    ],
    b"\x02": [  # Topic record
        ("version", 1),
        ("name_length", UNSIGNED_VARINT),
        ("name", LENGTH_PREVIOUS_UVARINT),
        ("topic_uuid", 16),
        ("tagged_fields_count", 1),
        ("headers_array_count", 1),
    ],
    b"\x03": [  # Partition record
        ("version", 1),
        ("partition_id", 4),
        ("topic_uuid", 16),
        ("length_replica_array", UNSIGNED_VARINT),
        ("replica_array_item", 4),
        ("length_in_sync_replica_array", UNSIGNED_VARINT),
        ("in_sync_replica_array_item", 4),
        ("length_removing_replicas_array", UNSIGNED_VARINT),
        ("removing_replicas_array_item", 4),
        ("length_adding_replicas_array", UNSIGNED_VARINT),
        ("adding_replicas_array_item", 4),
        ("leader", 4),
        ("leader_epoch", 4),
        ("partition_epoch", 4),
        ("length_directories_array", UNSIGNED_VARINT),
        ("directories_array_item", 16),
        ("tagged_fields_count", 1),
        ("headers_array_count", 1),
    ],
}
record_types = {b"\x0c": "Feature level", b"\x02": "Topic", b"\x03": "Partition"}
# Batch records from command line
batches = []
def process_batches(log_data):
    position = 0
    while position < len(log_data):
        print("BATCH", "-" * 20)
        batch = {}
        for field_name, field_length in batch_fields:
            batch[field_name] = log_data[position : position + field_length]
            print(f"{field_name}[{field_length}]: {batch[field_name]}")
            if field_name == "records_length":
                num_records = int.from_bytes(batch[field_name])
                print(f"{int.from_bytes(batch[field_name])} records in batch")
                record_position = position + field_length
                batch["records"] = []
                for i in range(0, num_records):
                    print("\tRECORD", "-" * 20)
                    record_data, number_bytes = process_record(
                        log_data[record_position:]
                    )
                    batch["records"].append(record_data)
                    record_position += number_bytes
                    print(f"{number_bytes} bytes in record number {i}")
                    print(
                        f"Record raw length {hex(int.from_bytes(record_data['length']))} decoded length {record_data['length_decoded']}"
                    )
                position = record_position
                field_length = 0
            position = position + field_length
        batches.append(batch)
def process_record(data):
    index = 0
    record = {}
    for field_name, field_length in record_fields:
        print(f"\tField: {field_name}", end="")
        if field_length == SIGNED_VARINT:
            # signed VARINT
            raw_data, num_bytes, decoded_value = decode_svarint(data[index:])
            record[field_name] = raw_data
            record[field_name + "_decoded"] = decoded_value
            index += num_bytes
            print(
                f" signed VARINT Raw: {record[field_name]} Decoded: {record[field_name + '_decoded']}"
            )
        elif field_length == UNSIGNED_VARINT:
            # Unsigned varint
            raw_data, num_bytes, decoded_value = decode_uvarint(data[index:])
            record[field_name] = raw_data
            record[field_name + "_decoded"] = decoded_value
            index += num_bytes
            print(
                f" unsigned VARINT Raw: {record[field_name]} Decoded: {record[field_name + '_decoded']}"
            )
        else:
            record[field_name] = data[index : index + field_length]
            index += field_length
            print(f" Normal length: {field_length} value: {record[field_name]}")
    # Specific fields according to record type
    actual_field = 0
    print(f"\t\tSPECIFIC fields for Type: {record_types[record['type']]}")
    for field_name, field_length in specific_fields[record["type"]]:
        print(f"\t\t{field_name}[{field_length}]", end="")
        if field_name.endswith("_array_item"):
            # Item of array of <PREVIOUS_UNSIGNED_VARINT> - 1 items, each with size <FIELD_LENGTH>
            previous_name = specific_fields[record["type"]][actual_field - 1][0]
            array_length = int(record[previous_name + "_decoded"]) - 1
            record[field_name] = []
            print(f" Array of {array_length} items")
            for i in range(array_length):
                record[field_name].append(data[index : index + field_length])
                index += field_length
                print(f"\t\t\tItem: {record[field_name][i]}")
        elif field_length == LENGTH_PREVIOUS:
            # Field length in previous field
            previous_name = specific_fields[record["type"]][actual_field - 1][0]
            field_length = int(record[previous_name]) - 1
            record[field_name] = data[index : index + field_length]
            index += field_length
            print(f" length in previous: {field_length} value: {record[field_name]}")
        elif field_length == SIGNED_VARINT:
            # sVARINT
            raw_data, num_bytes, decoded_value = decode_svarint(data[index:])
            record[field_name] = raw_data
            record[field_name + "_decoded"] = decoded_value
            index += num_bytes
            print(
                f" signedVARINT Raw: {record[field_name]} Decoded: {record[field_name + '_decoded']}"
            )
        elif field_length == UNSIGNED_VARINT:
            # Unsigned VARINT
            raw_data, num_bytes, decoded_value = decode_uvarint(data[index:])
            record[field_name] = raw_data
            record[field_name + "_decoded"] = decoded_value
            index += num_bytes
            print(
                f" unsignedVARINT Raw: {record[field_name]} Decoded: {record[field_name + '_decoded']}"
            )
        elif field_length == LENGTH_PREVIOUS_UVARINT:
            # field_length in previous field that is uVARINT
            previous_name = specific_fields[record["type"]][actual_field - 1][0]
            field_length = int(record[previous_name + "_decoded"]) - 1
            record[field_name] = data[index : index + field_length]
            index += field_length
            print(
                f" length in previous UVARINT: {field_length} value: {record[field_name]}"
            )
        else:
            record[field_name] = data[index : index + field_length]
            index += field_length
            print(f" normal field value: {record[field_name]}")
        actual_field += 1
    return record, index
"""
	https://protobuf.dev/programming-guides/encoding/
	signed VARINT
"""
def decode_svarint(data):
    shift = 0
    value = 0
    aux = MSB_SET_MASK
    index = 0
    record = b""
    while aux & MSB_SET_MASK:
        aux = data[index]
        record += aux.to_bytes()
        value += (aux & REMOVE_MSB_MASK) << shift
        index += 1
        shift += 7
    lsb = value & 0x01
    if lsb:
        value = -1 * ((value + 1) >> 1)
    else:
        value = value >> 1
    return record, index, value
"""
	https://protobuf.dev/programming-guides/encoding/
	unsigned VARINT
"""
def decode_uvarint(data):
    shift = 0
    value = 0
    aux = MSB_SET_MASK
    index = 0
    record = b""
    while aux & MSB_SET_MASK:
        aux = data[index]
        record += aux.to_bytes()
        value += (aux & REMOVE_MSB_MASK) << shift
        index += 1
        shift += 7
    return record, index, value
# -----------------------------------------------------------------------------
def make_body_ApiVersions(data):
    # error_code
    body = int(0).to_bytes(2, "big")
    # Array size INT32
    # ????????????????
    # COMPACT_ARRAY: size + 1 como UNSIGNED_VARINT
    # Number of api versions + 1
    # 4 for
    #  Include DescribeTopicPartitions in APIVersions #yk1
    body += int(4).to_bytes(1, "big")
    # ARRAY: size as INT32, does not work
    # body += int(1).to_bytes(4, "big")
    # api version 18, minversion(4), maxversion(>=4) 3 INT16
    body += (
        int(18).to_bytes(2, "big")
        + int(4).to_bytes(2, "big")
        + int(4).to_bytes(2, "big")
    )
    # tagged?? INT32
    # codecrafters challenge:
    # The value for this will always be a null byte in this challenge (i.e. no tagged fields are present)
    body += b"\x00"
    # api version 1 (Fetch), minversion(4), maxversion(>=16) 3 INT16
    body += (
        int(1).to_bytes(2, "big")
        + int(4).to_bytes(2, "big")
        + int(16).to_bytes(2, "big")
    )
    # tagged?? INT32
    # codecrafters challenge:
    # The value for this will always be a null byte in this challenge (i.e. no tagged fields are present)
    body += b"\x00"
    #  Include DescribeTopicPartitions in APIVersions #yk1
    # api version 75, minversion(0>=0), max(version>=0)
    body += int(75).to_bytes(2) + int(0).to_bytes(2) + int(0).to_bytes(2)
    body += b"\x00"
    # ---
    # throttle_time_ms INT32
    body += int(0).to_bytes(4, "big")
    # tagged?? INT32
    body += b"\x00"
    return body
def make_body_Fetch(data):
    max_wait_ms = data[0:4]
    min_bytes = data[4:8]
    max_bytes = data[8:12]
    isolation_level = data[12]
    session_id = data[13:17]
    session_epoch = data[17:21]
    # [topics] = topic_id [partitions] TAG_BUFFER
    # size of topics = UNSIGNED_VARINT
    # NO HAY ???
    topics_size = data[21]
    # UUID
    topic_id = data[22:38]
    #  ...
    print("Topics size: ", topics_size)
    print("Topic id: ", topic_id)
    # throttle_time_ms
    body = int(0).to_bytes(4, "big")
    # error_code
    body += int(0).to_bytes(2, "big")
    # session_id
    # body += session_id
    body += int(0).to_bytes(4)
    # RESPONSES-----------------------------
    # Beware! It seems that uses 1 for 0 topics instead of 0 per Kafka's doc
    # 0 Responses
    if topics_size == 0 or topics_size == 1:
        print("No topic")
        body += int(0).to_bytes(1, "big")
    else:
        # 1 Response
        body += b"\x02"
        # Topic
        body += topic_id
        # Partitions 1 element
        body += b"\x02"
        # Partion index INT32
        body += b"\x00\x00\x00\x00"
        # Error code 100 UNKNOWN_TOPIC INT16
        body += int(100).to_bytes(2)
        body += int(0).to_bytes(8, byteorder="big")  # high watermark
        body += int(0).to_bytes(8, byteorder="big")  # last stable offset
        body += int(0).to_bytes(8, byteorder="big")  # log start offset
        body += int(0).to_bytes(1, byteorder="big")  # num aborted transactions
        body += int(0).to_bytes(4, byteorder="big")  # preferred read replica
        body += int(0).to_bytes(1, byteorder="big")  # num records
        # TAG_BUFFER Partition
        body += b"\x00"
        # TAG_BUFFER Topic
        body += b"\x00"
    # TAG_BUFFER Final
    body += b"\x00"
    return body
def search_topic_name(topic_name: bytes):
    # Batches is GLOBAL
    for batch in batches:
        for record in batch["records"]:
            if record_types[record["type"]] == "Topic":
                if record["name"] == topic_name:
                    return record["topic_uuid"]
    return ""
def search_partitions(topic_uuid: bytes):
    # REMEMBER First field must be number of partitions
    # Watcha!!!
    # the length N + 1 is given as an >>> UNSIGNED_VARINT <<<
    # TODO: Fix it, we are supposing N fits in 1 byte ( N < 128 )
    p_body = b""
    number_partitions = 0
    for batch in batches:
        for record in batch["records"]:
            if record_types[record["type"]] == "Partition":
                if record["topic_uuid"] == topic_uuid:
                    p_data = int(NO_ERROR).to_bytes(2)
                    p_data += record["partition_id"]
                    p_data += record["leader"]
                    p_data += record["leader_epoch"]
                    # Replica nodes
                    number_nodes = record["length_replica_array_decoded"]
                    # TODO: UNSIGNED VARINT, we have it in raw
                    p_data += int(number_nodes).to_bytes(1)
                    for i in range(number_nodes - 1):
                        p_data += record["replica_array_item"][i]
                    # In sync replica nodes
                    number_nodes = record["length_in_sync_replica_array_decoded"]
                    # TODO: UNSIGNED VARINT, we have it in raw
                    p_data += int(number_nodes).to_bytes(1)
                    for i in range(number_nodes - 1):
                        p_data += record["in_sync_replica_array_item"][i]
                    # [eligible_leader_replicas] [last_known_elr] [offline_replicas]
                    # Suppose 0 for each one
                    p_data += b"\x01"
                    p_data += b"\x01"
                    p_data += b"\x01"
                    p_body += p_data
                    number_partitions += 1
    # [partitions] = <Prefix N+1> <partition array> <TAG_BUFFER>
    number_partitions += 1
    p_body = int(number_partitions).to_bytes(1) + p_body + b"\x00"
    return p_body
def make_body_DescribeTopicPartitions(data):
    """
    max_wait_ms = data[0:4]
    min_bytes = data[4:8]
    max_bytes = data[8:12]
    isolation_level = data[12]
    session_id  = data[13:17]
    session_epoch = data[17:21]
    # [topics] = topic_id [partitions] TAG_BUFFER
    # size of topics = UNSIGNED_VARINT
    # NO HAY ???
    """
    topics_size = data[0]
    topic_name_size = data[1]
    topic_name = data[2 : 2 + topic_name_size - 1]
    topic_tag_buffer = data[2 + topic_name_size - 1]
    max_partitions_response = data[2 + topic_name_size : 2 + topic_name_size + 4]
    cursor = data[2 + topic_name_size + 4]
    tag_buffer = data[2 + topic_name_size + 4 + 1]
    print("Topics size:", topics_size)
    print("Topic name:", topic_name)
    """
	To check if a topic exists or not
	, you'll need to read the __cluster_metadata topic's log file
	, located at 
	/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
	
	process_batches(log_data)
	"""
    topic_uuid = search_topic_name(topic_name)
    if topic_uuid:
        print(f"Found topic_uuid: {topic_uuid} for topic: {topic_name}")
        error_code = int(NO_ERROR).to_bytes(2)
        partitions_body = search_partitions(topic_uuid)
    else:
        error_code = int(UNKNOWN_TOPIC_OR_PARTITION).to_bytes(2)
        topic_uuid = int(0).to_bytes(16)
        partitions_body = int(1).to_bytes(1)
    # Build response body =================================================
    # throttle_time_ms
    body = int(0).to_bytes(4, "big")
    # topics length + 1
    body += b"\x02"
    # error_code
    # List for an unknown topic #vt6
    body += int(UNKNOWN_TOPIC_OR_PARTITION).to_bytes(2, "big")
    body += error_code
    # Same topic_name than in request
    # COMPACT_NULLABLE_STRING = size + 1
    # Topic_name_size includes +1
    body += int(topic_name_size).to_bytes()
    body += topic_name
    # topic_id 00000000-0000-0000-0000-000000000000 UUID
    # body += '00000000-0000-0000-0000-000000000000'.
    body += int(0).to_bytes(16)
    body += topic_uuid
    # is_internal
    body += int(0).to_bytes(1)
    # partitions
    body += int(1).to_bytes(1)
    # Body built in search_partitions
    body += partitions_body
    # tag_buffer
    # body += int(0).to_bytes(1)
    # Topic authorized operations
    """
    This corresponds to the following operations:
        READ (bit index 3 from the right)
        WRITE (bit index 4 from the right)
        CREATE (bit index 5 from the right)
        DELETE (bit index 6 from the right)
        ALTER (bit index 7 from the right)
        DESCRIBE (bit index 8 from the right)
        DESCRIBE_CONFIGS (bit index 10 from the right)
        ALTER_CONFIGS (bit index 11 from the right)
    """
    READ (bit index 3 from the right)
		WRITE (bit index 4 from the right)
		CREATE (bit index 5 from the right)
		DELETE (bit index 6 from the right)
		ALTER (bit index 7 from the right)
		DESCRIBE (bit index 8 from the right)
		DESCRIBE_CONFIGS (bit index 10 from the right)
		ALTER_CONFIGS (bit index 11 from the right)
	"""
    # https://github.com/apache/kafka/blob/1962917436f463541f9bb63791b7ed55c23ce8c1/clients/src/main/java/org/apache/kafka/common/acl/AclOperation.java#L44
    body += b"\x00\x00\x0d\xf8"
    # TAG_BUFFER_TOPICS
    body += b"\x00"
    # Next cursor
    # 0xff == NULL_VALUE,
    body += b"\xff"
    # TAG_BUFFER RESPONSE
    body += b"\x00"
    """
    # RESPONSES-----------------------------
    # Beware! It seems that uses 1 for 0 topics instead of 0 per Kafka's doc
    # 0 Responses
    if topics_size == 0 or topics_size == 1:
        print("No topic")
        body += int(0).to_bytes(1, "big")
    else:
        # 1 Response
        body += b"\x02"
        
        # Topic
        body += topic_id
        
        # Partitions 1 element
        body += b"\x02"
        
        # Partion index INT32
        body += b"\x00\x00\x00\x00"
        
        # Error code 100 UNKNOWN_TOPIC INT16
        body += int(100).to_bytes(2)
        body += int(0).to_bytes(8, byteorder="big") # high watermark 
        body += int(0).to_bytes(8, byteorder="big") # last stable offset
        body += int(0).to_bytes(8, byteorder="big") # log start offset
        
        body += int(0).to_bytes(1, byteorder="big") # num aborted transactions
        body += int(0).to_bytes(4, byteorder="big") # preferred read replica 
        body += int(0).to_bytes(1, byteorder="big") # num records 
        
        # TAG_BUFFER Partition
        body += b'\x00'
        # TAG_BUFFER Topic
        body += b'\x00'
   
    # TAG_BUFFER Final
    body += b'\x00'
    """
    	# RESPONSES-----------------------------
	# Beware! It seems that uses 1 for 0 topics instead of 0 per Kafka's doc
	# 0 Responses
	if topics_size == 0 or topics_size == 1:
		print("No topic")
		body += int(0).to_bytes(1, "big")
	else:
		# 1 Response
		body += b"\x02"
		
		# Topic
		body += topic_id
		
		# Partitions 1 element
		body += b"\x02"
		
		# Partion index INT32
		body += b"\x00\x00\x00\x00"
		
		# Error code 100 UNKNOWN_TOPIC INT16
		body += int(100).to_bytes(2)
		body += int(0).to_bytes(8, byteorder="big") # high watermark 
		body += int(0).to_bytes(8, byteorder="big") # last stable offset
		body += int(0).to_bytes(8, byteorder="big") # log start offset
		
		body += int(0).to_bytes(1, byteorder="big") # num aborted transactions
		body += int(0).to_bytes(4, byteorder="big") # preferred read replica 
		body += int(0).to_bytes(1, byteorder="big") # num records 
		
		# TAG_BUFFER Partition
		body += b'\x00'
  		# TAG_BUFFER Topic
		body += b'\x00'
  	# TAG_BUFFER Final
	body += b'\x00'
	"""
    return body
"""
	Cluster metadata records
	https://github.com/apache/kafka/tree/5b3027dfcbcb62d169d4b4421260226e620459af/metadata/src/main/resources/common/metadata
	Cluster metadata log file viewer
	https://binspec.org/kafka-cluster-metadata-log-file
	
"""
def handle(data):
    print("Request:\n", data, "\n")
    tam = data[0:4]  # length field
    request_api_key = data[4:6]
    request_api_version = data[6:8]
    correlation_id = data[8:12]
    # Rest of fields ignored !!!
    # Fetch with an unknown topic
    client_id_size = data[12:14]
    isize = int.from_bytes(client_id_size)
    if isize > 0:
        client_id = data[14 : 14 + isize]
        tagged = data[14 + isize]
    else:
        client_id = ""
        tagged = data[14]
    print(f"client_id = {client_id}")
    req_index = 14 + isize + 1
    # client_id
    # tagged_fields
    version = int.from_bytes(request_api_version, signed=True)
    # print(request_api_version, f'version(int) = {version}')
    api_key = int.from_bytes(request_api_key)
    if version in [0, 1, 2, 3, 4, 16]:
        # error code INT16
        # body = int(0).to_bytes(2, "big")
        if api_key == API_VERSIONS:
            print("ApiVersions")
            body = make_body_ApiVersions(data[12:])
        elif api_key == FETCH:
            print("Fetch")
            body = make_body_Fetch(data[req_index:])
        elif api_key == DESCRIBE_TOPIC_PARTITIONS:
            print("DescribeTopicPartitions")
            body = make_body_DescribeTopicPartitions(data[req_index:])
    else:
        print(f"Error! version: {version} request_api_version: ", request_api_version)
        body = int(35).to_bytes(2, "big")
    # Fetch with an unknown topic #hn6
    # Espera Response header v1:
    # correlation_id TAG_BUFFER
    size = 4 + len(body)
    if api_key == FETCH or api_key == DESCRIBE_TOPIC_PARTITIONS:
        # Response header v2
        size += 1
    header = size.to_bytes(4, "big") + correlation_id
    if api_key == FETCH:
        # Response header v2
        header += b"\x00"
    if api_key == DESCRIBE_TOPIC_PARTITIONS:
        # Response header v0
        header += b"\x00"
    print(f"Size: {size}")
    print("Header: ", end="")
    print(header)
    print("Header size: ", len(header))
    print("Body: ")
    print(body)
    return header + body
def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")
    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    logfile = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
    with open(logfile, "rb") as f:
        log_data = f.read()
    # batches variable is GLOBAL
    process_batches(log_data)
    print(f"{len(batches)} batches in file")
    while True:
        client, addr = server.accept()  # wait for client
        pid = os.fork()
        if pid == 0:
            # Child
            server.close()
            while True:
                r = client.recv(1024)
                client.sendall(handle(r))
        print(f"New process: {pid}")
        client.close()
if __name__ == "__main__":
    main()