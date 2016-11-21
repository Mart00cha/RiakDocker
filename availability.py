import unittest
import riak 
from threading import Thread
from time import sleep
from docker import Client
import pprint


class Availabilitytests(unittest.TestCase):

	def test_shouldFailToRead(self):
		pp = pprint.PrettyPrinter(indent=4)

		docker = Client()

		zomg = docker.exec_create(container=u'riak01', cmd="riak-admin status | grep ring")
		print docker.exec_start(zomg)

		myClient = riak.RiakClient(pb_port=12101, protocol='pbc')
		myBucket = myClient.bucket('test')

		val1 = "one"
		key1 = myBucket.new('one', data=val1)
		key1.store(w=3)

		val2 = "two"
		key2 = myBucket.new('two', data=val2)
		key2.store(w=3)

		val3 = "three"
		key3 = myBucket.new('three', data=val3)
		key3.store(w=3)

		fetched1 = myBucket.get('one', r=3)
		fetched2 = myBucket.get('two', r=3)
		fetched3 = myBucket.get('three', r=3)

		self.assertTrue("one" == fetched1.data) 
		self.assertTrue( "two" == fetched2.data)
		self.assertTrue("three" == fetched3.data)

		print fetched1.data
		print fetched2.data
		print fetched3.data

		docker.stop(u'riak01')
		docker.stop(u'riak02')
		docker.stop(u'riak03')

		try:
		  fetched1 = myBucket.get('one', r=3)
		except Exception as e: 
		  self.assertTrue(str(e) == "[Errno 61] Connection refused")
		  print e

		try:
		  fetched2 = myBucket.get('two', r=3)
		except Exception as e: 
		  self.assertTrue( str(e) == "[Errno 61] Connection refused")
		  print e

		try:
		  fetched3 = myBucket.get('three', r=3)
		except Exception as e: 
		  self.assertTrue(str(e) == "[Errno 61] Connection refused")
		  print e

		try:
			val4 = "four"
			key4 = myBucket.new('four', data=val4)
			key4.store(w=3)
		except Exception as e: 
		  self.assertTrue(str(e) == "[Errno 61] Connection refused")
		  print e

		docker.start(u'riak01')
		docker.start(u'riak02')
		docker.start(u'riak03')

		sleep(30)

		fetched1 = myBucket.get('one', r=3)
		fetched2 = myBucket.get('two', r=3)
		fetched3 = myBucket.get('three', r=3)
		val4 = "four"
		key4 = myBucket.new('four', data=val4)
		key4.store(w=3)
		fetched4 = myBucket.get('four', r=3)

		self.assertTrue("one" == fetched1.data) 
		self.assertTrue( "two" == fetched2.data)
		self.assertTrue("three" == fetched3.data)
		self.assertTrue("four" == fetched4.data)


def main():
	unittest.main()

if __name__ == '__main__':
    unittest.main()