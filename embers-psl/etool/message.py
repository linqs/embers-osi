#!/usr/bin/env python

import logging
import hashlib
import dateutil.parser
import datetime

log = logging.getLogger(__name__)

def add_embers_ids(obj, parent_id=None, derived_ids=None):
    '''
    Create an EMBERS identifier for an object if none exists. 
    The id is just a SHA1 hash of the object content.
    '''
    if not obj.has_key('embersId'):
        obj['embersId'] = hashlib.sha1(str(obj)).hexdigest()

    if parent_id:
        obj['parentId'] = parent_id

    if derived_ids:
        obj['derivedIds'] = derived_ids

    return obj

def normalize_date(obj, path):
    '''
    Find a date field in an object and convert it to
    a UTC ISO formatted date and write it back as 
    the 'date' field of the object.
    path - a field name, or array of field names describing the path to the source field.
    '''
    if obj.has_key('date'):
        dateStr = obj.get('date', None)
        if not dateStr is None:
            return obj

    value = None
    if isinstance(path, basestring):
        value = obj.get(path, None)
    else:
        tmp = obj
        for p in path:
            if isinstance(tmp, dict):
                tmp = tmp.get(p, None) 
        
        value = tmp

    result = None
    if isinstance(value, basestring):
        try:
            dt = dateutil.parser.parse(value)
            tt = dt.utctimetuple()
            # this is painful, but the only way I could figure to normalize the date
            # naive dates (e.g. datetime.now()) will have no conversion
            dt = datetime.datetime(*tt[0:6])
            result = dt.isoformat()
        except Exception as e:
            log.exception('Could not parse date "%s"', value)
    if isinstance(value, (int, float)):
        try:
            dt = datetime.datetime.utcfromtimestamp(value)
            result = dt.date().isoformat()
        except:
            log.exception('Could not parse date "%f"', value)
        
    if not result:
        result = datetime.datetime.utcnow().isoformat()
        
    obj['date'] = result
    return obj

def is_us_tweet(tweet):
    '''
    Detect tweets with US place marks.
    Currently just uses the 'place' indicator from twitter.
    Should tolerate Datasift or public API tweets. 
    (Datasift embeds the tweet in the 'tweet' field)
    '''
    n = tweet
    for k in ['twitter', 'place', 'country_code']:
        if n and k in n:
            n = n[k]

    return n == 'US'


def clean(msg):
    """Take a string that contains a JSON message and fix all of the odd bits in it.
    Throw exceptions if it doesn't go well."""
    assert isinstance(msg, dict), "Message must be a python dictionary."

    if 'date' not in msg:
        if msg.get('interaction', {}).get('created_at'):
            msg = normalize_date(msg, ["interaction", "created_at"])
        elif msg.get('created_at'):
            msg = normalize_date(msg, ["created_at"])
        else:
            msg = normalize_date(msg, 'published')

    # legacy messages
    if msg.get('embers_id') and not msg.get('embersId'):
        msg['embersId'] = msg['embers_id']
        del msg['embers_id']

    if is_us_tweet(msg):
        log.warn('Supressing US tagged tweet id=%s' % (msg.get('id_str') or msg.get('twitter', {}).get('id', 'UNKNOWN'),))
        return None

    msg = add_embers_ids(msg)

    return msg
