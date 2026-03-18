from rustycluster import get_client, RustyClusterConfig

# 1. Get a client (authenticates automatically)
client = get_client()

# 2. Use it
client.set("hello", "world")
print(client.get("hello"))   # "world"
client.hset("hash_key", "field_key", "12")
print(client.hget("hash_key", "field_key"))  # 1
# None
client.close()