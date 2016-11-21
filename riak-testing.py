import unittest
import riak 

class Availabilitytests(unittest.TestCase):

	@classmethod
	def setUpClass(self):
		self.myClient = riak.RiakClient(pb_port=12101, protocol='pbc')
		self.myBucket = self.myClient.bucket('test')

	def test_writing_and_reading(self):
		val1 = "one"
		key1 = self.myBucket.new('one', data=val1)
		key1.store()

		val2 = "two"
		key2 = self.myBucket.new('two', data=val2)
		key2.store()

		val3 = "three"
		key3 = self.myBucket.new('three', data=val3)
		key3.store()

		fetched1 = self.myBucket.get('one')
		fetched2 = self.myBucket.get('two')
		fetched3 = self.myBucket.get('three')

		self.assertTrue(val1 == fetched1.data) 
		self.assertTrue( val2 == fetched2.data)
		self.assertTrue( val3 == fetched3.data)

		print fetched1.data
		print fetched2.data
		print fetched3.data

	def test_update(self):
		fetched3 = self.myBucket.get('three')
		fetched3.data = "four"
		fetched3.store()

		fetched4 = self.myBucket.get('three')

		self.assertTrue("four" == fetched4.data) 

	def test_delete(self):
		fetched1 = self.myBucket.get('one')
		fetched2 = self.myBucket.get('two')
		fetched3 = self.myBucket.get('three')

		fetched1.delete()
		fetched2.delete()
		fetched3.delete()

		self.assertTrue( self.myBucket.get('one').exists == False)
		self.assertTrue( self.myBucket.get('two').exists == False)
		self.assertTrue( self.myBucket.get('three').exists == False)

def main():
	unittest.main()

if __name__ == '__main__':
    unittest.main()