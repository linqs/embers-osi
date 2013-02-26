#!/usr/bin/env python

import sys
import os
import argparse

def get_common_args():
    '''
    Return an ArgumentParser instance with all of the common arguments added.
    This is suitable for input as the parent argument of an ArgumentParser constructor.
    '''
    ap = argparse.ArgumentParser('EMBERS common arguments.', add_help=False)

    # Queue arguments
    ap.add_argument('--pub', metavar='PUBLISH', type=str, nargs='+', required=False,
                    help='Queue names or URLs to publish on.')
    ap.add_argument('--sub', metavar='SUBSCRIBE', type=str, nargs='+', required=False,
                    help='Queue names or URLs to subscribe to.')
    ap.add_argument('--queue-conf', metavar='QUEUE_CONF', type=str, nargs='?', required=False,
                    help='Location of the queue configuration file.')    

    # SSH arguments
    ap.add_argument('--tunnel', metavar='TUNNEL', type=str, required=False,
                    help='An SSH connection string, e.g. "embers@ec2-50-16-120-255.compute-1.amazonaws.com", to open a tunnel connection to the queue. Use with --key.')
    ap.add_argument('--ssh-key', metavar='KEYFILE', type=str, required=False,
                    help='The path to an SSH-compatible key file to use as the connection credentials for --tunnel. Use with --tunnel.')

    # AWS values
    ap.add_argument('--aws-key', metavar='AWSKEY', type=str, default=os.environ.get('AWS_ACCESS_KEY_ID', None),
                    help='''The AWS key.''')
    ap.add_argument('--aws-secret', metavar='AWSSECRET', type=str, default=os.environ.get('AWS_SECRET_ACCESS_KEY', None),
                    help='''The AWS key secret.''')

    # General stuff
    ap.add_argument('--verbose', action="store_true",
                    help="Write debug messages.")
    
    return ap

def get_parser(description=None):
    '''
    Create an ArgumentParser instance with the common arguments included. 
    See argparse.ArgumentParser.add_argument() for details on adding your own arguments.
    Use parse_args() to get the argument values object. (See argparse.ArgumentParser.parse_args()).
    The simplest usage, if you only use default args, is 'args = get_parser.parse_args()'
    '''
    return argparse.ArgumentParser(description=description, parents=[get_common_args()])        
