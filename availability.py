import unittest
import riak 
from threading import Thread
from time import sleep
from docker import Client
import pprint


class Availabilitytests(unittest.TestCase):

	@classmethod
	def setUpClass(self):
		pp = pprint.PrettyPrinter(indent=4)

		self.docker = Client()

		zomg = self.docker.exec_create(container=u'riak01', cmd="riak-admin status | grep ring")
		print self.docker.exec_start(zomg)

		myClient = riak.RiakClient(pb_port=12101, protocol='pbc')
		self.myBucket = myClient.bucket('test')

	def test_Read_fail_Read(self):
		val1 = "one"
		key1 = self.myBucket.new('one', data=val1)
		key1.store(w=3)

		val2 = "two"
		key2 = self.myBucket.new('two', data=val2)
		key2.store(w=3)

		val3 = "three"
		key3 = self.myBucket.new('three', data=val3)
		key3.store(w=3)

		fetched1 = self.myBucket.get('one', r=3)
		fetched2 = self.myBucket.get('two', r=3)
		fetched3 = self.myBucket.get('three', r=3)

		self.assertTrue("one" == fetched1.data) 
		self.assertTrue( "two" == fetched2.data)
		self.assertTrue("three" == fetched3.data)

		print fetched1.data
		print fetched2.data
		print fetched3.data

		print "Stopping 3 nodes..."

		self.docker.stop(u'riak01')
		self.docker.stop(u'riak02')
		self.docker.stop(u'riak03')

		
		with self.assertRaises(Exception):
			fetched1 = self.myBucket.get('one', r=3)
		
		with self.assertRaises(Exception):
		  fetched2 = self.myBucket.get('two', r=3)
		
		with self.assertRaises(Exception):
		  fetched3 = self.myBucket.get('three', r=3)
		
		with self.assertRaises(Exception):
			val4 = "four"
			key4 = self.myBucket.new('four', data=val4)
			key4.store(w=3)

		print "Read failed as expected"

		print "Starting 3 nodes..."
		self.docker.start(u'riak01')
		self.docker.start(u'riak02')
		self.docker.start(u'riak03')

		sleep(30)

		fetched1 = self.myBucket.get('one', r=3)
		fetched2 = self.myBucket.get('two', r=3)
		fetched3 = self.myBucket.get('three', r=3)
		val4 = "four"
		key4 = self.myBucket.new('four', data=val4)
		key4.store(w=3)
		fetched4 = self.myBucket.get('four', r=3)

		self.assertTrue("one" == fetched1.data) 
		self.assertTrue( "two" == fetched2.data)
		self.assertTrue("three" == fetched3.data)
		self.assertTrue("four" == fetched4.data)


def main():
	unittest.main()

if __name__ == '__main__':
    unittest.main()