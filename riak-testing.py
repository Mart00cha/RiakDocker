import riak 

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