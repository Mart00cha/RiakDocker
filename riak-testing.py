import riak 

#basic functionality testing - read, write, delete objects
myClient = riak.RiakClient(pb_port=12101, protocol='pbc')
myBucket = myClient.bucket('test')

val1 = "one"
key1 = myBucket.new('one', data=val1)
key1.store()

val2 = "two"
key2 = myBucket.new('two', data=val2)
key2.store()

val3 = "three"
key3 = myBucket.new('three', data=val3)
key3.store()

fetched1 = myBucket.get('one')
fetched2 = myBucket.get('two')
fetched3 = myBucket.get('three')

assert val1 == fetched1.data
assert val2 == fetched2.data
assert val3 == fetched3.data

print fetched1.data
print fetched2.data
print fetched3.data

fetched3.data = "three"
fetched3.store()

fetched1.delete()
fetched2.delete()
fetched3.delete()

assert myBucket.get('one').exists == False
assert myBucket.get('two').exists == False
assert myBucket.get('three').exists == False


#sibling explosion simulation
client1 = riak.RiakClient(pb_port=12101, protocol='pbc')
client2 = riak.RiakClient(pb_port=12102, protocol='pbc')

bucket1 = client1.bucket('test1')
bucket2 = client2.bucket('test2')

key1 = bucket1.new("a", "Bob")
key1.store()
fetched1 = bucket1.get("a")

key2 = bucket2.new("a", "Sue")
key2.store()
fetched2 = bucket1.get("a")

fetched1.data = "Rita"
fetched1.store()

fetched2.data = "Michelle"
fetched2.store()

print myBucket.get("a")

#enable dotted vec

riak-admin bucket-type dotted_vec '{"props":{"dvv_enabled":true}}'
riak-admin bucket-type dotted_vec

