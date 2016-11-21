import unittest
import riak 
from threading import Thread
from time import sleep
from docker import Client
import pprint


class Availabilitytests(unittest.TestCase):

	def test_shouldFailToRead(self):

		pp = pprint.PrettyPrinter(indent=4)

		gkey1 = None
		gkey2 = None

		docker = Client()

		zomg = docker.exec_create(container=u'riak01', cmd="riak-admin status | grep ring")
		print docker.exec_start(zomg)

		zomg = docker.exec_create(container=u'riak01', cmd="riak-admin bucket-type create vectors '{\"props\":{\"allow_mult\": true, \"dvv_enabled\": false}}'")
		print docker.exec_start(zomg)

		zomg = docker.exec_create(container=u'riak01', cmd="riak-admin bucket-type update vectors '{\"props\":{\"allow_mult\": true, \"dvv_enabled\": false}}'")
		print docker.exec_start(zomg)

		zomg = docker.exec_create(container=u'riak01', cmd="riak-admin bucket-type activate vectors")
		print docker.exec_start(zomg)

		zomg = docker.exec_create(container=u'riak01', cmd="riak-admin bucket-type create dots '{\"props\":{\"allow_mult\": true, \"dvv_enabled\": true}}'")
		print docker.exec_start(zomg)

		zomg = docker.exec_create(container=u'riak01', cmd="riak-admin bucket-type update dots '{\"props\":{\"allow_mult\": true, \"dvv_enabled\": true}}'")
		print docker.exec_start(zomg)

		zomg = docker.exec_create(container=u'riak01', cmd="riak-admin bucket-type activate dots")
		print docker.exec_start(zomg)


		client1 = riak.RiakClient(pb_port=12101, protocol='pbc')
		bucket1 = client1.bucket('vec1', bucket_type='vectors')
		client2 = riak.RiakClient(pb_port=12102, protocol='pbc')
		bucket2 = client2.bucket('vec1', bucket_type='vectors')

		key1 = bucket1.new("a", "Bob")
		gkey1 = key1.store()
		for i in gkey1.siblings:
			print "Y " + i.data

		key2 = bucket2.new("a", "Sue")
		gkey2 = key2.store()
		for i in gkey2.siblings:
			print "X " + i.data

		gkey1.siblings = []
		gkey1.data = "Rita"
		gkey1.store()
		gkey2.siblings = []
		gkey2.data = "Michelle"
		gkey2.store()


		sleep(5)
		obj = bucket1.get("a")

		for i in obj.siblings:
			print i.data

		obj.delete()

		print "\n"

		client1 = riak.RiakClient(pb_port=12101, protocol='pbc')
		bucket1 = client1.bucket('dot', bucket_type='dots')
		client2 = riak.RiakClient(pb_port=12101, protocol='pbc')
		bucket2 = client2.bucket('dot', bucket_type='dots')

		key1 = bucket1.new("a", "Bob")
		gkey1 = key1.store()
		for i in gkey1.siblings:
			print "Y " + i.data

		key2 = bucket2.new("a", "Sue")
		gkey2 = key2.store()
		for i in gkey2.siblings:
			print "X " + i.data

		gkey1.siblings = []
		gkey1.data = "Rita"
		gkey1.store()
		gkey2.siblings = []
		gkey2.data = "Michelle"
		gkey2.store()

		sleep(5)
		obj = bucket1.get("a")

		for i in obj.siblings:
			print i.data

		obj.delete()

def main():
	unittest.main()

if __name__ == '__main__':
    unittest.main()
