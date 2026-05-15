from rustycluster import close_all, get_client

# Fetch a client for a specific cluster declared in rustycluster.yaml.
db0 = get_client("DB0")
db0.set("ask:user1", "world")
print(db0.get("ask:user1"))                       # "world"
db0.hset("ask:hash_key", "field_key", "12")
print(db0.hget("ask:hash_key", "field_key"))      # "12"

# A second cluster is just a different name; same API.
db1 = get_client("DB1")
db1.set("chat:user1", "from-db1")
print(db1.get("chat:user1"))                       # "from-db1"

close_all()
