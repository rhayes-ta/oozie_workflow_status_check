#!/usr/bin/env python


'''
For info on the Oozie REST API, try 
https://oozie.apache.org/docs/3.1.3-incubating/WebServicesAPI.html#Jobs_Information

Here's a sample URL to fetch coordinator status for user t4b:
http://prodmaster01d.hdp.tripadvisor.com:11000/oozie/v1/jobs?filter=user%3Dt4b&jobtype=coordinator&len=100

except jobtype=wf or jobtype=coordinator
for workflow jobs, the appName matches the coordinators' coordJobName

This can be used in name= filter, example (to get workflows in coordinator job t4b-review-dashboard user=t4b)
http://prodmaster01d.hdp.tripadvisor.com:11000/oozie/v1/jobs?filter=user%3Dt4b;name%3Dt4b-review-dashboard&jobtype=wf
'''

import sys
import urllib
import json
import argparse
from enum import Enum, IntEnum
import pprint


class NagiosState(IntEnum):
    Ok = 0
    Warn = 1
    Error = 2
    Unknown = 3


class OozieStatus(Enum):
    FAILED = 'FAILED'
    KILLED = 'KILLED'
    SUSPENDED = 'SUSPENDED'
    SUCCEEDED = 'SUCCEEDED'
    RUNNING = 'RUNNING'
    PREP = 'PREP'

    def to_nagios(self):
        if self is OozieStatus.FAILED:
            return NagiosState.Error
        elif self is OozieStatus.KILLED:
            return NagiosState.Warn
        elif self is OozieStatus.SUSPENDED:
            return NagiosState.Warn
        else:
            return NagiosState.Ok


def oozie_status_to_nagios(oozie_status):
    try:
        os = OozieStatus(oozie_status)
        return os.to_nagios()
    except:
        return NagiosState.Unknown


class Oozie():
    def __init__(self, host='localhost', port=11000):
        self.host = host
        self.port = port

    def _url(self):
        return "http://%s:%d/oozie/v1/" % (self.host, self.port)

    def _apply(self, path, params):
        base_url = self._url()
        url = base_url + path + '?' + urllib.urlencode(params)
        raw_json = urllib.urlopen(url)
        return json.load(raw_json)

    def _checkJobtype(self,jobtype):
        if jobtype in ('wf','coordinator','bundle'):
            return jobtype
        raise Exception('unknown jobtype')

    def _encodeFilter(self, filter):
        """
        :param filter: Dict, valid keys are name, user, group, status
        """
        ff = [ '%s=%s' % (k,v) for k,v in filter.iteritems() ]
        return ";".join(ff)

    def jobs(self, filter={}, jobtype='wf', len=100, offset=1):
        params = {
            'filter':self._encodeFilter(filter),
            'jobtype':self._checkJobtype(jobtype),
            'len':len,
            'offset':offset
        }
        return self._apply('jobs', params)

    def _coordinator_runs_per_day(self, timeUnit, frequency):
        """
        How many coordinator runs per day?  Returns float.

        >>> self = Oozie()
        >>> self._coordinator_runs_per_day(u'DAY', '1')
        1.0
        >>> self._coordinator_runs_per_day(u'DAY', '2')
        0.5
        >>> self._coordinator_runs_per_day(u'MINUTE', '360')
        4.0

        """

        multiplier = 1
        if timeUnit == u'DAY':
            multiplier = 1
        elif timeUnit == u'MINUTE':
            multiplier = 24*60
        elif timeUnit == u'HOUR':
            multiplier = 24
        elif timeUnit == u'WEEK':
            multiplier = 1.0/7.0
        elif timeUnit == u'MONTH':
            multiplier = 1.0/30.0
        return multiplier / float(frequency)

    def coordinator(self, days, filter={}):
        fd = ExtensibleDict(filter)
        coords = self.jobs(filter=fd.copy_with(status='RUNNING'), jobtype='coordinator')
        cc = coords.get('coordinatorjobs')
        for c in cc:
            timeUnit = c.get('timeUnit')
            frequency = c.get('frequency')
            # determine how many workflows to fetch
            number_to_fetch = int(days * self._coordinator_runs_per_day(timeUnit,frequency))
            # sadly, the coordinator name (coordJobName) may not be the same as the appName
            # in the child workflow.
            coordPath = c.get('coordJobPath','')
            appName = coordPath.split('/')[-1]
            ftr = fd.copy_with(name=appName)
            the_jobs = self.jobs(ftr, jobtype='wf',len=number_to_fetch)
            c['workflows'] = the_jobs.get('workflows')
            # looks like workflows are returned in reverse createdTime order, newest first. Yay.
        return cc

    # TODO Handle pagination, stepping through toplevel.offset by toplevel.len until we get to toplevel.total

class ExtensibleDict(dict):
    """
    A dictionary which can supply an extended copy of itself, incorporating kwargs additional or replacement keys

    >>> ed = ExtensibleDict({'a':1,'b':2})
    >>> ed == {'a':1, 'b':2}
    True
    >>> ed.copy_with(c=3) == {'a':1,'b':2,'c':3}
    True
    >>> ed == {'a': 1, 'b': 2}
    True
    >>> ed.copy_with(b=100) ==  {'a': 1, 'b': 100}
    True
    >>> ed.copy_with(c=3,delta=100,gamma='Gramma') == {'a':1,'b':2,'c':3,'delta':100,'gamma':'Gramma'}
    True

    """
    def __init__(self, *args, **kwargs):
        super(ExtensibleDict, self).__init__(*args, **kwargs)

    def copy_with(self, **kwargs):
        new_dict = ExtensibleDict(self)
        new_dict.update(**kwargs)
        return new_dict


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Get status of Oozie jobs')
    parser.add_argument('host')
    parser.add_argument('-p','--port', type=int, default=11000)
    parser.add_argument('-l','--limit', type=int, default=100, help='Limit on number of coordinators to check')
    parser.add_argument('-u', '--user', help='User name to filter')
    parser.add_argument('-n', '--name', help='Name to filter')
    parser.add_argument('-d', '--days', type=int, help="how many days back to examine workflow history", default=3)
    parser.add_argument('-v', '--verbose', action='count', default=0,
                      help='increase output verbosity (use up to 3 times)')

    args = parser.parse_args()

    try:
        oozie = Oozie(args.host)
        filter={}
        if args.user:
            filter['user'] = args.user
        if args.name:
            filter['name'] = args.name
        cc = oozie.coordinator(args.days, filter)

        exitCode = NagiosState.Ok.value

        for c in cc:
            coordJobName = c.get('coordJobName')
            coordStatus = c.get('status')

            workflows = c.get('workflows', [])
            for wf in workflows:
                wfStatus = wf.get('status')
                wfId = wf.get('id')
                wfStartTime = wf.get('startTime')
                nag = oozie_status_to_nagios(wfStatus)

                if args.verbose > 2:
                    print wf
                elif args.verbose == 2:
                    print coordJobName, coordStatus, wfStatus, wfId, '"%s"' % wfStartTime
                exitCode = max(exitCode,nag)

            if args.verbose == 1:
                if workflows:
                    print coordJobName, len(workflows), (max( [ oozie_status_to_nagios(wf.get('status')) for wf in workflows] )).name
                else:
                    print coordJobName, len(workflows), NagiosState.Unknown.name

        sys.exit(exitCode)

    except Exception as problem:
        print problem
        sys.exit(NagiosState.Unknown)

