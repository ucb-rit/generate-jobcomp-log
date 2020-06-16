#!/usr/bin/python2
import urllib2
import urllib
import json
import time
import string
from collections import defaultdict


VERSION = 0.1
docstr = '''
[version: {}]
'''.format(VERSION)

BASE_URL = 'http://mybrc.brc.berkeley.edu/mybrc-rest/'
# BASE_URL = 'https://scgup-dev.lbl.gov:8443/mybrc-rest'
# BASE_URL = 'http://localhost:8880/mybrc-rest'

line_template = '''JobId={jobid} UserId={username}({userid}) GroupId={groupname}(groupid) Name={name} JobState={jobstate} Partition={partition} TimeLimit={timelimit} 
StartTime={starttime} EndTime={endtime} NodeList={nodelist} NodeCnt={node_count} ProcCnt={proc_count} WorkDir={workdir} ReservationName={reservation_name} Gres={g_res} 
Account={account} QOS={qos} WcKey={wc_key} Cluster={cluster} SubmitTime={submittime} EligibleTime={eligibletime} ArrayJobId={array_jobid} ArrayTaskId={array_taskid} 
DerivedExitCode={derived_exitcode} ExitCode={exitcode}'''
# jobid, username, userid, groupname, groupid, name, jobstate, partition,
# timelimit, starttime, endtime, nodelist, nodecnt, proccnt, workdir,
# reservationname, gres, account, qos, wckey, cluster, submittime,
# eligibletime, arrayjobid, arraytaskid, derivedexitcode, exitcode


timestamp_format = '%Y-%m-%dT%H:%M:%S'


def process_date_time(date_time):
    return time.mktime(time.strptime(date_time, timestamp_format)) if date_time else None


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'


def get_job_url(start, end, user, account, page=1):
    request_params = {
        'page': page
    }

    if start:
        request_params['start_time'] = start

    if end:
        request_params['end_time'] = end

    if user:
        request_params['user'] = user

    if account:
        request_params['account'] = account

    url_usages = BASE_URL + '/jobs?' + \
        urllib.urlencode(request_params)
    return url_usages


def paginate_req_table(url_function, params=[None, None, None, None]):
    req = urllib2.Request(url_function(*params))
    response = json.loads(urllib2.urlopen(req).read())

    table = response['results']
    page = 2
    while response['next'] is not None:
        try:
            req = urllib2.Request(url_function(*params, page=page))
            response = json.loads(urllib2.urlopen(req).read())

            yield response['results']
            # table.extend(response['results'])
            page += 1
        except urllib2.URLError:
            response['next'] = None

    # return table


# jobs = paginate_req_table(get_job_url)


# jobid, username, userid, groupname, groupid, name, jobstate, partition,
# timelimit, starttime, endtime, nodelist, nodecnt, proccnt, workdir,
# reservationname, gres, account, qos, wckey, cluster, submittime,
# eligibletime, arrayjobid, arraytaskid, derivedexitcode, exitcode

# for job in jobs:
for batch in paginate_req_table(get_job_url):
    for job in batch: 
        jobid = job['jobslurmid']
        userid = job['userid']
        jobstate = job['jobstatus']
        partition = job['partition']
        starttime = process_date_time(job['startdate'][:-1])
        endtime = process_date_time(job['enddate'][:-1])
        nodelist = job['nodes']
        nodecnt = job['num_alloc_nodes']
        proccnt = job['num_cpus']
        qos = job['qos']
        submittime = process_date_time(job['submitdate'][:-1])

        # accountid = job['accountid']

        # username              : get using userid
        # groupname
        # groupid
        # name
        # timelimit
        # wckey
        # cluster
        # workdir
        # reservationname
        # gres
        # account               : get using accountid
        # eligibletime
        # arrayjobid
        # arraytaskid
        # derivedexitcode
        # exitcode

        print string.Formatter() \
            .vformat(line_template, (),
                     SafeDict(jobid=jobid,
                              userid=userid,
                              jobstate=jobstate,
                              partition=partition,
                              starttime=starttime, endtime=endtime,
                              nodelist=str(nodelist),
                              nodecnt=nodecnt, proccnt=proccnt,
                              qos=qos,
                              submittime=submittime))

