import unittest
from . import mock_channel


class TestServer(mock_channel.Server):

  def set_receiver(self, receiver):
    self.receiver = receiver

  def send(self, msg):
    self.receiver(msg + 1)


class TestReceiver:

  def __call__(self, msg):
    self.msg = msg


class TestObject:

  def set_receiver(self, receiver):
    self.receiver = receiver

  def foo(self, *args, **kw):
    self.args = args
    self.kw = kw


class MockChannelTests(unittest.TestCase):

  def testMockChannel(self):
    recorder = mock_channel.Recorder()
    wrapper = mock_channel.ServerWrapper(server=TestServer(), recorder=recorder)

    receiver = TestReceiver()
    wrapper.set_receiver(receiver)
    wrapper.send(2)

    self.assertEqual(receiver.msg, 3)

    self.assertEqual({'send': [2], 'receive': [3]}, recorder.data)

    mock_wrapper = mock_channel.ServerWrapper(
      server=None,
      mock_server=mock_channel.ReplayRequestReply(recorder.data))
    mock_wrapper.set_receiver(receiver)
    mock_wrapper.send(2)

    self.assertEqual(receiver.msg, 3)

  def testMethodCall(self):
    recorder = mock_channel.Recorder()
    target = TestObject()

    wrapper = mock_channel.ServerWrapper(
      server=mock_channel.MethodCallDeserializer(target), recorder=recorder)

    receiver = TestReceiver()
    wrapper.set_receiver(receiver)

    serializer = mock_channel.MethodCallSerializer(wrapper, target)
    serializer.foo('bar', 'baz', blah=3)
    self.assertEqual(target.args, ('bar', 'baz'))
    self.assertEqual(target.kw, {'blah': 3})

    target.receiver('foo')
    self.assertEqual(receiver.msg, 'foo')


if __name__ == '__main__':
  unittest.main()
