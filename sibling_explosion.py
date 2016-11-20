import riak 
from threading import Thread
from time import sleep
from docker import Client
import pprint

pp = pprint.PrettyPrinter(indent=4)

def threaded_function(arg, bucket):
	if arg==1:
		key1 = bucket.new("a", "Bob")
		key1 = key1.store()
		for i in key1.siblings:
			print "1" + i.data
		key1.siblings = [key1.siblings[0]]
		key1.data = "Rita"
		key1.store()
	else:
		key2 = bucket.new("a", "Sue")
		key2 = key2.store()
		for i in key2.siblings:
			print "2" + i.data
		key2.siblings = [key2.siblings[0]]
		key2.data = "Michelle"
		key2.store()


docker = Client()

zomg = docker.exec_create(container=u'riak01', cmd="riak-admin status | grep ring")
print docker.exec_start(zomg)

zomg = docker.exec_create(container=u'riak01', cmd="riak-admin bucket-type create vectors '{\"props\":{\"allow_mult\": true}, \"dvv_enabled\": false}'")
print docker.exec_start(zomg)

zomg = docker.exec_create(container=u'riak01', cmd="riak-admin bucket-type update vectors '{\"props\":{\"allow_mult\": true}, \"dvv_enabled\": false}'")
print docker.exec_start(zomg)

zomg = docker.exec_create(container=u'riak01', cmd="riak-admin bucket-type activate vectors")
print docker.exec_start(zomg)

zomg = docker.exec_create(container=u'riak01', cmd="riak-admin bucket-type create dots '{\"props\":{\"allow_mult\": true}, \"dvv_enabled\": true}'")
print docker.exec_start(zomg)

zomg = docker.exec_create(container=u'riak01', cmd="riak-admin bucket-type update dots '{\"props\":{\"allow_mult\": true}, \"dvv_enabled\": true}'")
print docker.exec_start(zomg)

zomg = docker.exec_create(container=u'riak01', cmd="riak-admin bucket-type activate dots")
print docker.exec_start(zomg)


client1 = riak.RiakClient(pb_port=12101, protocol='pbc')
bucket1 = client1.bucket('vec', bucket_type='vectors')
client2 = riak.RiakClient(pb_port=12101, protocol='pbc')
bucket2 = client2.bucket('vec', bucket_type='vectors')
thread1 = Thread(target = threaded_function, args = [1, bucket1])
thread2 = Thread(target = threaded_function, args = [2, bucket2])
thread1.start()
thread2.start()
thread1.join()
thread2.join()


obj = bucket1.get("a")

for i in obj.siblings:
	print i.data

obj.delete()

print "\n"

client1 = riak.RiakClient(pb_port=12101, protocol='pbc')
bucket1 = client1.bucket('dot', bucket_type='dots')
client2 = riak.RiakClient(pb_port=12101, protocol='pbc')
bucket2 = client2.bucket('dot', bucket_type='dots')
thread1 = Thread(target = threaded_function, args = [1, bucket1])
thread2 = Thread(target = threaded_function, args = [2, bucket2])
thread1.start()
thread2.start()
thread1.join()
thread2.join()


obj = bucket1.get("a")

for i in obj.siblings:
	print i.data

obj.delete()


