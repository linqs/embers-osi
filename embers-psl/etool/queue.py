#!/usr/bin/env python

import sys
import __builtin__
import zmq
import zmq.ssh
import json
import logging
import hashlib
import os
import os.path
import codecs
import time

log = logging.getLogger(__name__)
conf = {}

# constant to select bind() for attaching the socket
BIND = 1
# constant to select connect() for attaching the socket
CONNECT = 2

def init(args=None):
    # init logger
    # load/get the config
    # eventually this needs a search path for the config
    # should be env(QFU_CONFIG);./queue.conf;/etc/embers/queue.conf;tcp://localhost:3473
    # use 3473 as the global control channel
    global conf

    cf = None
    if args and vars(args).get('queue_conf', None):
        log.debug('trying queue config %s', vars(args)['queue_conf'])
        cf = vars(args)['queue_conf']

    if not (cf and os.path.exists(cf)):
        log.debug('trying queue config %s', os.path.join(os.getcwd(), 'queue.conf'))
        cf = os.path.join(os.getcwd(), 'queue.conf')
        
    if not (cf and os.path.exists(cf)):
        log.debug('trying queue config %s', os.path.join(os.environ.get('HOME', '.'), 'queue.conf'))
        cf = os.path.join(os.environ.get('HOME', '.'), 'queue.conf')
        
    if not (cf and os.path.exists(cf)):
        log.debug('trying queue config %s', os.path.join(os.path.dirname(sys.argv[0]), 'queue.conf'))
        cf = os.path.join(os.path.dirname(sys.argv[0]), 'queue.conf')
        
    if not (cf and os.path.exists(cf)):
        log.warn('Could not find queue.conf, bailing out.')
        return

    try:
        with __builtin__.open(cf, 'r') as f:
            conf = json.load(f)
    except Exception as e:
        log.exception("Could not find or load config file %s", (cf,))
    
    log.debug('loaded config=%s from "%s"', conf, cf)

class JsonMarshal(object):
    def __init__(self, encoding='utf8'):
        # raises an error if you get a bogus encoding
        codecs.lookup(encoding) 
        self.encoding = encoding

    def encode(self, obj):
        return json.dumps(obj, encoding=self.encoding, ensure_ascii=False)

    def decode(self, data):
        return json.loads(data, encoding=self.encoding)

    def send(self, socket, data):
        socket.send_unicode(data, encoding=self.encoding)

    def recv(self, socket):
        return socket.recv_unicode(encoding=self.encoding)

class UnicodeMarshal(JsonMarshal):
    def __init__(self, **kw):
        super(UnicodeMarshal, self).__init__(**kw)

    def encode(self, obj):
        return unicode(obj)

    def decode(self, data):
        # exception if this is not decodeable (str, stream etc.)
        return unicode(data)

    # send and recv are handled in JsonMarshall

class RawMarshal(object):
    def encode(self, obj):
        return obj

    def decode(self, obj):
        return obj

    def send(self, socket, data):
        if isinstance(data, unicode):
            socket.send_unicode(data)
        else:
            socket.send(data)

    def recv(self, socket):
        return socket.recv()

class StreamCaptureProbe(object):
    def __init__(self, encoding='utf8', stream=sys.stdout):
        self._s = codecs.getwriter(encoding)(stream)
        self._s.flush() # make sure its good

    def __call__(self, action, message):
        if action == Queue.SENT:
            self._s.write(message)
            self._s.write('\n')
            self._s.flush()

class QueueStatsProbe(object):
    def __init__(self, interval_min=5):
        self.interval = datetime.timedelta(minutes=interval_min)
        self.start = datetime.datetime.now()
        self.sent_bytes = 0
        self.sent_msg = 0
        self.recv_bytes = 0
        self.recv_msg = 0

    def __call__(self, action, message):
        if action == Queue.SENT:
            self.sent_bytes += len(message)
            self.sent_msg += 1

        if action == Queue.RECEIVED:
            self.recv_bytes += len(message)
            self.recv_msg += 1

        # TODO - if delta past period report the stats

class Queue(object):
    # constants
    SENT = 1
    RECEIVED = 2
    
    def __init__(self, addr, qtype, 
                 context=None, 
                 marshal=JsonMarshal(), 
                 hwm=10000, 
                 attach=None,
                 ssh_key=None, 
                 ssh_conn=None,
                 use_paramiko=True):
        '''
        addr - ZMQ-style URL
        qtype - ZMQ queue type constant (e.g. zmq.PUB)
        context - ZMQ context (one is created if None)
        marshal - a marshaller for the messages (encode, decode, recv and send)
        hwm - ZMQ High Water Mark (default 10000 messages)
        attach - the type of socket attachment - use constants BIND or CONNECT
        ssh_key - optional SSH key file for SSH tunnel connections
        ssh_conn - optional SSH connection string (user@hostname) for SSH tunnel connections
        use_paramiko - use the Paramiko SSH library if True.
        '''
        # cope with lists
        if isinstance(addr, basestring):
            self._addr = [addr]
        else:
            self._addr = addr 

        self._qtype = qtype
        self._context = context or zmq.Context()
        self._err = None
        self._socket = None
        self._marshal = marshal
        self._hwm = hwm
        self._ssh_key = ssh_key
        self._ssh_conn = ssh_conn
        self._use_paramiko = use_paramiko
        self._probes = [] # probes for tracing events
        self._attach = attach
        # invariants
        assert self._marshal, "No marshaller defined."
        assert self._addr, "No endpoint address defined."

    def open(self):
        if self._socket and not self._socket.closed:
            self.close()

        self._socket = self._context.socket(self._qtype)
        self._socket.setsockopt(zmq.HWM, self._hwm)
        
        if not self._attach:
            if self._qtype in (zmq.PUB, zmq.REP):
                self._attach = BIND
            elif self._qtype in (zmq.SUB, zmq.REQ):
                self._attach = CONNECT

        for a in self._addr:
            if self._attach == BIND:
                self._socket.bind(a)
                log.debug('bind: %s HWM=%d' % (a, self._hwm))
                time.sleep(0.5) # HACK: need proper startup here
            elif self._attach == CONNECT:
                if self._ssh_key and self._ssh_conn:
                    zmq.ssh.tunnel_connection(self._socket, a, 
                                              server=self._ssh_conn, 
                                              keyfile=self._ssh_key, 
                                              paramiko=self._use_paramiko)
                    log.debug('tunnel_connection: %s HWM=%d, server=%s' % (a, self._hwm, self._ssh_conn))
                else:
                    self._socket.connect(a)
                    log.debug('connect: %s HWM=%d' % (a, self._hwm))

            else:
                raise Exception('Invalid attachment type %s. Use BIND or CONNECT.' % (str(self._attach),))
                
        if self._qtype == zmq.SUB:
            self._socket.setsockopt(zmq.SUBSCRIBE, '')


    def check_socket(self):
        if not self._socket:
            raise Exception('Socket not created.')

        if self._socket.closed:
            raise Exception('Socket is closed.')

        if self._err:
            raise self._err

    def close(self):
        if self._socket and not self._socket.closed:
            self._socket.close()
            self._socket = None

    ## basic file semantics
    def read(self):
        self.check_socket()
        msg = self._marshal.recv(self._socket)
        result = self._marshal.decode(msg)
        self.notify(Queue.RECEIVED, msg)
        return result

    def write(self, obj):
        self.check_socket()
        msg = self._marshal.encode(obj)
        self._marshal.send(self._socket, msg)
        self.notify(Queue.SENT, msg)
    
    ## be an iterator
    ## http://docs.python.org/library/stdtypes.html#iterator-types
    def __iter__(self):
        return self 

    def next(self):
        try:
            self.check_socket()
        except Exception:
            raise StopIteration

        return self.read()

    ## support contextmanager
    ## see http://docs.python.org/library/stdtypes.html#context-manager-types
    ## with queue.open(...) as q: ...
    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_val, ex_trace):
        self.close()
        # tell any open control channels we are exiting
        return False

    ## probes for tracing messages
    ## this is how you can do dumps of messages as they are read/written
    ## and stuff like collecting metrics on messages
    def add_probe(self, probe):
        assert hasattr(probe, '__call__'), "Object must be callable."
        self._probes.append(probe)

    def notify(self, action, msg):
        for p in self._probes:
            try:
                p(action, msg)
            except KeyboardInterrupt:
                raise

            except:
                log.exception('Failed to notify probe.')

class StreamQueue(object):
    """An object to make a stream (typically stdin or stdout)
    conform to the Queue interface so we can write code that treats
    them interchangeably."""

    def __init__(self, stream, 
                 mode='r',
                 encoding='utf8',
                 marshal=JsonMarshal(),
                 end_of_record = '\n'):
        assert stream, "Need to a stream to read or write to."
        assert marshal, "Need a message marshaller to encode and decode messages."

        self._marshal = marshal
        self.end_of_record = end_of_record
        if encoding:
            if mode == 'w':
                self._stream = codecs.getwriter(encoding)(stream, 'replace')
            else: # default read
                self._stream = codecs.getreader(encoding)(stream, 'replace')
        else: # accept what they give you
            self._stream = stream


    def read(self):
        msg = self._stream.readline().strip()
        result = self._marshal.decode(msg)
        return result


    def write(self, obj):
        if not obj:
            return

        msg = self._marshal.encode(obj).strip()
        self._stream.write(msg)
        self._stream.write(self.end_of_record)


    def __iter__(self):
        self._iter = self._stream.__iter__()
        return self

    def next(self):
        if self._iter:
            msg = self._iter.next()
            return self._marshal.decode(msg.strip())
        else:
            raise Exception('No iterator initialized')


    def __enter__(self):
        self._ctx = self._stream.__enter__()
        return self._ctx


    def __exit__(self, ex_type, ex_val, ex_trace):
        if self._ctx:
            return self._ctx.__exit__()
        else:
            return False


def resolve_address(qname, qtype=zmq.SUB, attach=None):
    """Resolve qname into a queue specification, 
    either from queue.conf or by treating it as a 
    fully qualified name if it is not in the conf.
    The attach parameter overrides the default attachment type
    (BIND or CONNECT) for queues doing special connections.
    """
    result = qname # assume full queue name, e.g. tcp://localhost:1234
    info = conf.get(qname, {})
    port = info.get('port', None)
    host = info.get('host', 'localhost')
    if port:
        # bind sockets will always use * as the hostname
        if attach == BIND:
            result = 'tcp://*:%d' % (port)
        elif attach == CONNECT:
            result = 'tcp://%s:%d' % (host, port)
        elif qtype in (zmq.PUB, zmq.REP): 
            result = 'tcp://*:%d' % (port)
        else:
            result = 'tcp://%s:%d' % (host, port)

    return result


def open(name, mode='r', capture=None, **kw):
    '''
    Open a queue with file-like semantics. E.g.:
    q = open('sample-1', 'w') - publish
    q = open('sample-1', 'r') - subscribe
    options:
    name - a queue name, either a full ZMQ-style URL or a name found in queue.conf
    mode - the queue open more. One of r (SUB), w (PUB), r+ (REP), w+ (REQ).
    marshal - class to use to marshal messages, default JsonMarshal
    capture - capture and log messages as they are sent. Can be True, or a stream, or a Capture instance.
    '''

    # this is somewhat goofy, but once you have 
    # a metaphor you might as well run it into the ground
    mode_map = {
        'r'  : zmq.SUB, # read only
        'w'  : zmq.PUB, # write only
        'r+' : zmq.REP, # read, then write
        'w+' : zmq.REQ, # write, then read
        'sub': zmq.SUB,
        'pub': zmq.PUB,
        'req': zmq.REQ,
        'rep': zmq.REP,
        'push': zmq.PUSH,
        'pull': zmq.PULL
        }
    typ = mode_map.get(mode, None)
    assert typ, 'Mode %s is not a valid mode. Use one of r, w, r+, w+ or a ZMQ type name (sub, pub, req, rep, push, pull).' % (mode,)

    addr = None
    if isinstance(name, basestring):
        addr = [resolve_address(name, qtype=typ, attach=kw.get('attach', None))]
    else:
        addr = [resolve_address(n, qtype=typ, attach=kw.get('attach', None)) for n in name]

    assert addr, "Could not resolve an address from %s." % (name,)

    result = Queue(addr, typ, **kw)
    result.open()
    if capture:
        result.add_probe(StreamCaptureProbe())

    return result


#initialize the module
#init()

def main():
    """A little utility to handle reading and writing streams 
    to and from a queue.
    --pub <queue> : publish what's read from stdin to <queue>
    --sub <queue> : read from <queue> and write the messages to stdout
    --cat         : when used with --pub, write all published messages to stdout
    --clean       : check in incoming and outgoing messages. 
                    Verify the message is correct JSON and add
                    an embersId if needed.
    Other standard EMBERS options (e.g. --verbose).
    """
    import args
    import logs
    import message

    ap = args.get_parser()
    ap.add_argument('--clean', action="store_true",
                    help='Verify message format and add standard fields such as embersId.')
    ap.add_argument('--cat', action="store_true",
                    help='Write all published messages to stdout.')
    arg = ap.parse_args()
    logs.init(arg)
    init(arg)
    assert arg.sub or arg.pub, "Need to subscribe or publish to something."

    # need to use the raw/utf handler unless we are doing clean
    marshal = UnicodeMarshal()
    if arg.clean:
        marshal = JsonMarshal()
    
    subq = None
    if arg.sub: # read a queue
        subq = open(arg.sub, 'sub', marshal=marshal, ssh_key=arg.ssh_key, ssh_conn=arg.tunnel)
    else: # read stdin
        subq = StreamQueue(sys.stdin, marshal=marshal)

    pubq = None
    if arg.pub: # send to queue
        pubq = open(arg.pub, 'pub', capture=arg.cat, marshal=marshal)
    else: # send to stdout
        pubq = StreamQueue(sys.stdout, mode='w', marshal=marshal)

    rc = 0
    try:
        it = subq.__iter__()
        while True:
            m = ''
            try:
                m = it.next()
                if arg.clean:
                    m = message.clean(m)
                    
                if m:
                    pubq.write(m)
                    
            except StopIteration:
                break

            except KeyboardInterrupt:
                break

            except:
                rc += 1
                if m:
                    log.exception('Could not process message %s' % (m,))
                else:
                    log.exception('Unknown processing error')

    except KeyboardInterrupt:
        pass

    except:
        rc = 1
        log.exception('Top level exception')

    return rc


if __name__ == '__main__':
    sys.exit(main())
