import abc
import threading
import typing
import enum
import queue


Receiver = typing.Callable[[object], None]


class Server(abc.ABC):

  @abc.abstractmethod
  def set_receiver(self, receiver: Receiver):
    raise NotImplementedError()

  @abc.abstractmethod
  def send(self, msg: object):
    raise NotImplementedError()


class Logger(abc.ABC):

  @abc.abstractmethod
  def log(self, msg: object):
    raise NotImplementedError()


class Direction(enum.Enum):
  SEND = 'send'
  RECEIVE = 'receive'


class ReplayError(Exception):
  """Raised when a message is unexpected."""


class ReplayRequestReply(threading.Thread, Server):

  def __init__(self, data):
    super(ReplayRequestReply, self).__init__()
    self._data = data
    self.index = 0
    self.q = queue.Queue()
    self.daemon = True
    self._receiver = None

  def set_receiver(self, receiver: Receiver):
    self._receiver = receiver

  def send(self, msg: object):
    self.q.put(msg)

  def run(self):
    while True:
      msg = self.q.get()
      if self.index >= len(self._data[Direction.SEND]):
        raise ReplayError(repr(msg))

      if self._data[Direction.SEND][self.index] != msg:
        raise ReplayError(repr(msg))

      if self.playback_index < len(self.data['receive']):
        self._receiver(self.data['receive'][self.playback_index])
        self.playback_index += 1


class Recorder:
  def __init__(self):
    self.data = {Direction.SEND.value: [], Direction.RECEIVE.value: []}

  def record(self, direction: Direction, msg: object):
    self.data[direction.value].append(msg)


class ServerWrapper:

  class Mode(enum.Enum):
    PROD = 'prod'
    PROD_VERIFY = 'prod_verify'
    RECORD = 'record'
    PLAYBACK = 'playback'

  class WrapperReceiver:

    def __init__(self, server_wrapper):
      self._server_wrapper = server_wrapper
      self._receiver = None

    def set_receiver(self, receiver: Receiver):
      assert self._receiver is None
      self._receiver = receiver

    def __call__(self, msg: object):
      self._server_wrapper._process(Direction.RECEIVE, msg, self._receiver)

  def __init__(self, server: Server, mock_server: Server = None, recorder: Recorder = None):
    self._recorder = None
    if server is not None:
      self.mode = self.Mode.PROD
      self._server = server
      if recorder is not None:
        self.mode = self.Mode.RECORD
        self._recorder = recorder
    elif mock_server is not None:
      self.mode = self.Mode.PLAYBACK
      self._server = mock_server
    else:
      assert False

    self._receiver = self.WrapperReceiver(self)
    self._server.set_receiver(self._receiver)

  def set_receiver(self, receiver: Receiver):
    self._receiver.set_receiver(receiver)

  def send(self, msg: object):
    self._process(Direction.SEND, msg, self._server.send)

  def _process(self, direction: str, msg: object, receiver: Receiver):
    if self._recorder is not None:
      self._recorder.record(direction, msg)
    receiver(msg)


class ClassAttributeAccessError(Exception):
  pass


class MethodCallSerializer:

  def __init__(self, server, wrapped, namespace=''):
    self._server = server
    self._wrapped = wrapped
    self._namespace = namespace

  def __getattr__(self, attr):
    if attr[0] == '_':
      return super(self, ObjectWrapper).__getattr__(attr)

    value = getattr(self._wrapped, attr)
    if not callable(value) and not hasattr(self._wrapped.__class__, attr):
      raise ClassAttributeAccessError(attr)

    return MethodCallSerializer(self._server, value, attr)

  def __call__(self, *args, **kw):
    self._server.send((self._namespace, args, kw))


class MethodCallDeserializer(Server):

  def __init__(self, target):
    self._target = target

  def set_receiver(self, receiver):
    if (hasattr(self._target, 'set_receiver') and
        callable(self._target.set_receiver)):
      self._target.set_receiver(receiver)

  def send(self, msg):
    method, args, kw = msg
    getattr(self._target, method)(*args, **kw)
