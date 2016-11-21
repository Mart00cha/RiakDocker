import unittest
import riak 

class Availabilitytests(unittest.TestCase):

	def test_shouldFailToRead(self):
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

		self.assertTrue(val1 == fetched1.data) 
		self.assertTrue( val2 == fetched2.data)
		self.assertTrue( val3 == fetched3.data)

		print fetched1.data
		print fetched2.data
		print fetched3.data

		fetched3.data = "three"
		fetched3.store()

		fetched1.delete()
		fetched2.delete()
		fetched3.delete()

		self.assertTrue( myBucket.get('one').exists == False)
		self.assertTrue( myBucket.get('two').exists == False)
		self.assertTrue( myBucket.get('three').exists == False)

def main():
	unittest.main()

if __name__ == '__main__':
    unittest.main()