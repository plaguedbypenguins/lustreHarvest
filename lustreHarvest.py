#!/usr/bin/env python

# read OST/MDT lustre stats and fire them back via tcp to a server
#
# client sends are pretty closely synchronised so that the server can tell
# when a sweep is finished, do aggregation, and spoof close to coherent data
# into ganglia
# 
#   (c) Robin Humble - Sat Jun 23 15:12:33 EST 2012

# read lustre OSTs eg.
#   /proc/fs/lustre/obdfilter/data-OST0013/exports/10.1.99.4@o2ib/stats
# snapshot_time             1340428613.428605 secs.usecs
# read_bytes                47738 samples [bytes] 0 1048576 13585464050
# write_bytes               7681 samples [bytes] 5 1048576 5813192368
# ...
#
# or MDTs eg.
#   /proc/fs/lustre/{mds,mdt}/data-MDT0000/exports/10.1.14.1@o2ib/stats

import os, socket, select, sys, cPickle, time, subprocess, hashlib

port = 8022  # default port
clientSend = 3  # number of gathers per minute on clients
serverInterfaceName = None

# map between fs name in lustre and the name we want to see in ganglia
nameMap = { 'data':'vu_short',
            'apps':'vu_apps',
            'home':'vu_home',
          'images':'vu_images',
          'system':'vu_system',
           'gdata':'g_data' }

# names of cluster head nodes where server instances of this script run
head = { 'vu':'vu-man4', 'xe':'xepbs', 'dcc':'dccpbs' }

# lnets of each cluster
localLnets = { 'vu':'o2ib', 'xe':'o2ib2', 'dcc':'tcp102' }

# relaying servers send summed data to various other server instances.
# specify which clusters to relay to
relay = { 'alkindi': [ 'vu', 'xe', 'dcc' ] }

# stats file locations on oss/mds. handle v1.8 and 2.5
statsDir = { 'oss':['/proc/fs/lustre/obdfilter'], 'mds':['/proc/fs/lustre/mds', '/proc/fs/lustre/mdt'] }

verbose = 0
dryrun = 0

# shared secret between client and servers. only readable by root
secretFile = '/root/.lustreHarvest.secret'

gmondHost = '192.168.55.13' # g2 IPoIB # '239.2.11.71'   # multicast address or hostname or ip
gmondPort = 8650   # 8649
gmondProtocol = 'udp' # 'multicast'  # 'multicast' or 'udp'

dt = 60.0/clientSend
hostCache = {}
secretText = None

def getHost(ip):
   try:
      host = hostCache[ip]
   except:
      try:
         host = socket.gethostbyaddr(ip)[0]
      except:
         host = None
   hostCache[ip] = host
   return host

def spoofIntoGanglia(g, o, name, unit):
   if len(o) == 0 or dryrun:
      return
   for i, d in o.iteritems():
      # decode ip@lnet to a hostname
      ip = i.split('@')[0]
      host = getHost(ip)
      #print 'ip', ip, 'host', host
      if host == None:
         # if the host is unknown then it could be data for a different cluster. ignore it.
         #print >>sys.stderr, 'unknown host', i, ip
         continue
      spoofStr = ip + ':' + host
      #print 'g.send(', name, '%.2f', d, 'float', unit, 'both', 60, 0, '', spoofStr, ')'
      g.send( name, '%.2f' % d, 'float', unit, 'both', 60, 0, "", spoofStr )

def readSecret():
   global secretText
   try:
      l = open( secretFile, 'r' ).readlines()
   except:
      print >> sys.stderr, 'problem reading secret file:', secretFile
      sys.exit(1)
   # check for and complain about an empty secret file
   ok = 0
   for i in l:
      i = i.strip()
      if i != '':
         ok = 1
         break
   if not ok:
      print >> sys.stderr, 'nothing in the shared secret file:', secretFile
      sys.exit(1)
   secretText = str(l)

def computeRates( sOld, s, tOld, t ):
   err = 0
   if len(s) == 0:  # no data for this fs
      return {}, err
   deltat = t - tOld
   rates = {}

   # common case is that same clients are in new and old data. optimise this.
   sk = s.keys()
   sk.sort()
   sok = sOld.keys()
   sok.sort()
   if sk == sok:
      for h in s.keys():
         ds = s[h] - sOld[h]
         if ds < 0:
            print >>sys.stderr, 'negative rate', h, ds, s[h], sOld[h]
            err = 1
            ds = 0
         rates[h] = float(ds)/deltat
      return rates, err

   # if clients have changed (always added, never removed?) then do the full much slower loop
   for h in s.keys():
      if h in sOld.keys():
         ds = s[h] - sOld[h]
         if ds < 0:
            print >>sys.stderr, 'clients changed. negative rate', h, ds, s[h], sOld[h]
            err = 1
            ds = 0
         rates[h] = float(ds)/deltat
      else:
         rates[h] = 0.0
   return rates, err

def readStatsFile(fn):
   # turn into a dict
   f = open(fn, 'r')
   i = {}
   for ll in f:
      j = ll.split()
      i[j[0]] = j[1:]
   # ignore snapshot_time and ping, but harvest and sum all the other [reqs] and call them iops
   ops = None
   for n,j in i.iteritems():
      if len(j) < 3 or n in ( 'read_bytes', 'write_bytes', 'snapshot_time', 'ping' ):
         continue
      if j[2] == '[reqs]':
         if ops == None:
            ops = 0
         ops += int(j[0])
   # do read and write
   r = None
   if 'read_bytes' in i.keys():  # only populated in stats file if used
      r = int(i['read_bytes'][5])
   w = None
   if 'write_bytes' in i.keys():
      w = int(i['write_bytes'][5])
   return ( r, w, ops )

def gatherStats(fs):
   s = {}
   osts = []
   # handle both mds and oss
   for machType, lld in statsDir.iteritems():
      for ld in lld:  # loop over possible stats dirs looking for fsName-*
         try:
            dirs = os.listdir(ld)
         except:
            continue
         # find osts
         for d in dirs:
            if d[:len(fs)] == fs and len(d) > len(fs) and d[len(fs)] == '-':
               osts.append((machType, ld, d))

   for machType, ld, o in osts:
      s[o] = {}
      s[o]['type'] = machType   # oss or mds data
      ostDir = ld + '/' + o + '/exports'
      # loop over all clients
      for c in os.listdir(ostDir):
         #print c
         r, w, ops = None, None, None
         try:
            r, w, ops = readStatsFile(ostDir + '/' + c + '/stats')
         except:
            pass

         # don't report null osts
         if (r, w, ops) == (None, None, None):
            continue

         # we don't want to report oss<->oss or mds<->oss or mds<->mds traffic
         #   mds->oss has snapshot_time only,
         #      which is covered by the above None,None,None case.
         #   oss->{oss,mds} has no read_bytes or write_bytes in it.
         #      it may have eg. create/destry/setattr iops but we don't care.
         if machType == 'oss' and r == None and w == None:
            continue

         if r == None:
            r = 0
         if w == None:
            w = 0
         if ops == None:
            ops = 0

         s[o][c] = (r, w, ops)
         #print s[o][c]
   #print s
   return s

def uniq( list ):
   l = []
   prev = None
   for i in list:
      if i != prev:
         l.append( i )
      prev = i
   return l

def sumDataToClients(o, t):
   # check times across stats are recent
   tData = t
   for oss in o.keys():
      if o[oss]['time'] < tData:
         tData =  o[oss]['time']
      elif  t - o[oss]['time'] > dt:
         print >>sys.stderr, 'old oss data', oss, 'time',  o[oss]['time'], 'now', t
   #if t - tData > dt:
   #   print 'an ost had stale data by', t-tData
   if verbose:
      print 'stalest data', t - tData

   # count osts and filesystems
   Nost = 0
   fss = []
   for oss in o.keys():
      if o[oss]['dataType'] == 'relay':  # skip relay data
         continue
      if 'data' in o[oss].keys():
         if len(o[oss]['data'].keys()):
            fss.extend(o[oss]['data'].keys())
            for f in o[oss]['data'].keys():  # filesystems
               Nost += len(o[oss]['data'][f].keys())
         else:
            print >>sys.stderr, 'no filesystems found on', oss
      else:
         print >>sys.stderr, 'data not in oss keys of', oss, 'keys', o[oss].keys()
   #print 'oss', len(o.keys()), 'ost', Nost
   fss.sort()
   fss = uniq(fss)

   # build list of all clients
   c = []
   for oss in o.keys():
      if o[oss]['dataType'] == 'relay':  # skip relay data
         continue
      for f in o[oss]['data'].keys():  # filesystems
         for ost in o[oss]['data'][f].keys():
            c.extend(o[oss]['data'][f][ost].keys())
   c.sort()
   c = uniq(c)
   if 'type' in c:
      c.remove('type')
   if verbose:
      #print 'clients', len(c)
      print 'oss/mds', len(o.keys()), 'ost/mdt', Nost, 'clients', len(c), 'filesystems', fss
      #print 'client list', time.time() - t

   # make zero'd client lists
   r = {}
   w = {}
   ossOps = {}
   mdsOps = {}
   ostCnt = {}
   mdtCnt = {}
   for f in fss:
      r[f] = {}
      w[f] = {}
      ossOps[f] = {}
      mdsOps[f] = {}
      ostCnt[f] = 0
      mdtCnt[f] = 0
      for i in c:
         r[f][i] = 0
         w[f][i] = 0
         ossOps[f][i] = 0
         mdsOps[f][i] = 0

   # debug
   rTot, wTot, ossOpsTot, mdsOpsTot = {},{},{},{}
   for f in fss:
      rTot[f] = 0
      wTot[f] = 0
      ossOpsTot[f] = 0
      mdsOpsTot[f] = 0

   # sum across clients
   for oss in o.keys():
      if o[oss]['dataType'] == 'relay':  # skip relay data
         continue
      s = o[oss]['data'] # shorten for easier use
      for f in s.keys(): # filesystems
         for ost in s[f].keys():
            machType = s[f][ost]['type']  # oss or mds
            del s[f][ost]['type']
            if machType == 'oss':
               ostCnt[f] += 1
               for i in s[f][ost].keys():  # loop over clients
                  rc, wc, opsc = s[f][ost][i]
                  r[f][i] += rc
                  w[f][i] += wc
                  ossOps[f][i] += opsc
                  if verbose:  # info/debug
                     rTot[f] += rc
                     wTot[f] += wc
                     ossOpsTot[f] += opsc
            else:
               mdtCnt[f] += 1
               for i in s[f][ost].keys():  # loop over clients
                  rc, wc, opsc = s[f][ost][i]
                  r[f][i] += rc
                  w[f][i] += wc
                  mdsOps[f][i] += opsc
                  if verbose:  # info/debug
                     rTot[f] += rc
                     wTot[f] += wc
                     mdsOpsTot[f] += opsc
   if verbose:
      #print 'c', c
      for f in fss:
         print f, 'tot GB r,w, M ops mds,oss', rTot[f]/(1024*1024*1024), wTot[f]/(1024*1024*1024), mdsOpsTot[f]/(1024*1024), ossOpsTot[f]/(1024*1024)
      print 'client process time', time.time() - t

   # we are only monitoring the mdt for some fs's and in those cases don't
   # want any oss information to get back to servers
   for f in fss:
      if ostCnt[f] == 0 and mdtCnt[f] == 1:   # only mdt was found
         #print f, 'is mdt only'
         r[f] = {}
         w[f] = {}
         ossOps[f] = {}

   return r, w, ossOps, mdsOps, fss

def mergeRemotePreSummed(o, d):
   t = time.time()

   # optimise away the case where there is no remotely summed data
   rem = 0
   for oss in o.keys():
      if o[oss]['dataType'] == 'relay':  # found remote data
         rem = 1
   if not rem:
      return d

   # local data
   r, w, ossOps, mdsOps, fss = d

   for oss in o.keys():
      if o[oss]['dataType'] != 'relay':  # skip local data
         continue
      rRem, wRem, ossOpsRem, mdsOpsRem, fssRem = o[oss]['data']['d']

      rTot = 0
      wTot = 0
      ossOpsTot = 0
      mdsOpsTot = 0

      for f in fssRem:
         if f in fss:
            # not sure how this can happen...
            print >>sys.stderr, 'error. remote summed data from', oss, 'is for a local fs', f
            continue
         fss.append(f)
         r[f] = rRem[f]
         w[f] = wRem[f]
         ossOps[f] = ossOpsRem[f]
         mdsOps[f] = mdsOpsRem[f]

         if verbose:
            for c in r[f].keys():
               rTot += r[f][c]
            for c in w[f].keys():
               wTot += w[f][c]
            for c in ossOps[f].keys():
               ossOpsTot += ossOps[f][c]
            for c in mdsOps[f].keys():
               mdsOpsTot += mdsOps[f][c]
            print f, 'remote tot GB r,w, M ops mds,oss', rTot/(1024*1024*1024), wTot/(1024*1024*1024), mdsOpsTot/(1024*1024), ossOpsTot/(1024*1024)
   if verbose:
      print 'remote merge process time', time.time() - t

   return r, w, ossOps, mdsOps, fss

def zeroOss(o):
   o['size'] = -1
   # leave o['data'] intact

def removeProcessedData(o):
   for oss in o.keys():
      if 'data' in o[oss].keys():
         o[oss]['data'] = {}

def printRate(s,o):
   if len(o.keys()) == 0:
      return
   j = 0
   for i in o.keys():
      j += o[i]
   print s, j

def doRelaySend(rs, host, port, d):
   # bundle all data up into a message of dataType 'relay' and
   # send to other clusters. return connections also so we
   # can re-use them next time

   # check to see if we should be relaying anything to anywhere
   if host not in relay.keys():
      return

   # setup connections to the hosts we are relaying to
   for cluster in relay[host]:
      hn = head[cluster]
      if hn not in rs.keys() or rs[hn] == None:
         rs[hn] = connectSocket((hn, port))
         print >>sys.stderr, 'setting up new relay connection to', (hn,port)

   # construct message
   #   ... for now, send all data, regardless of lnet
   s = {}
   s['dataType'] = 'relay'
   s['d'] = d
   h, b = constructMessage(s)

   for cluster in relay[host]:
      hn = head[cluster]
      c = rs[hn]
      if c == None:
         continue
      try:
         c.send(h)
         c.send(b)
         #print 'relay sent', len(b)
      except:
         print >>sys.stderr, 'relay send of', len(h), len(b), 'failed'
         c.close()
         rs[hn] = None

   return rs


# notes:
#  - time skew and delay in data is necessary as need to recv, delay, sum and then send summed data
#    ie. relaying cluster needs to run with an offset of ~5s(?) before the others
#  - simple way to write it is to relay all summed data to all clusters ie. unecessarily large transmits,
#    but this is necessary in the 'zero knowledge of endpoints' relayer model
#  - central fs has only remote clients so most info it gathers is useful only for remote clusters.
#    however one meaningful number is the per oss data that could be dropped into central fs's ganglia

def serverCode( serverName, port ):
   import gmetric

   # Create a TCP/IP socket
   server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   server.setblocking(0)

   # Bind the socket to the port
   server_address = (serverName, port)
   print >>sys.stderr, 'starting up on %s port %s' % server_address
   server.bind(server_address)

   # Listen for incoming connections
   server.listen(1)

   # Sockets from which we expect to read
   inputs = [ server ]
   # Sockets to which we expect to write
   outputs = []

   # setup socket to talk to gmond
   g = gmetric.Gmetric( gmondHost, gmondPort, gmondProtocol )
   ## debug: send to nonsense destination port:
   #g = gmetric.Gmetric( '239.2.11.71', 8659, 'multicast' )

   o = {}
   fss = []

   r = {}
   w = {}
   ossOps = {}
   mdsOps = {}

   rRate = {}
   wRate = {}
   ossOpsRate = {}
   mdsOpsRate = {}

   tLast = time.time()  # the time we last got a block from clients
   processed = 1
   first = 1
   rs = {}  # relay sockets used to send to other clusters

   while inputs:
      # Wait for at least one of the sockets to be ready for processing
      #print >>sys.stderr, '\nwaiting for the next event'
      timeout = 1  # seconds
      readable, writable, exceptional = select.select(inputs, outputs, inputs, timeout)
      #readable, writable, exceptional = select.select(inputs, outputs, inputs)

      if not (readable or writable or exceptional):
         #print >>sys.stderr, '  timed out, do some other work here'
         # if the interval is long then just wait 5s, otherwise wait dt/2
         t = time.time()
         if not processed and t - tLast > min(5.0, dt/2):
             # process and fire into gmond
             rOld = r
             wOld = w
             ossOpsOld = ossOps
             mdsOpsOld = mdsOps
             fssOld = fss
             # sum all data from all servers to the clients
             d = sumDataToClients(o, t)

             # maybe relay some of the summed data to other server instances
             rs = doRelaySend(rs, serverName, port, d)

             # maybe merge remote pre-summed data into our local data
             d = mergeRemotePreSummed(o, d)

             r, w, ossOps, mdsOps, fss = d

             # remove data fields to avoid re-processing data from stopped oss's. not necessary??
             removeProcessedData(o)

             if fss != fssOld:
                first = 1

             err = 0
             if not first:
                if verbose:
                   print 'rate dt', tLast - tOld
                for f in fss:  # loop over each fs
                   if verbose:
                      print 'fs', f
                   t = time.time()
                   rRate[f], err1 = computeRates( rOld[f], r[f], tOld, tLast )
                   wRate[f], err2 = computeRates( wOld[f], w[f], tOld, tLast )
                   ossOpsRate[f], err3 = computeRates( ossOpsOld[f], ossOps[f], tOld, tLast )
                   mdsOpsRate[f], err4 = computeRates( mdsOpsOld[f], mdsOps[f], tOld, tLast )

                   # err indicates a negative rate. likely an ost/mdt failover or oss/mds reboot
                   fsErr = err1 or err2 or err3 or err4
                   err = err or fsErr

                   tRate = time.time() - t
                   t = time.time()
                   if verbose:
                      #print 'rRate', rRate[f]
                      #print 'wRate', wRate[f]
                      #print 'ossOpsRate', ossOpsRate[f]
                      #print 'mdsOpsRate', mdsOpsRate[f]
                      printRate('rRate', rRate[f])
                      printRate('wRate', wRate[f])
                      printRate('ossOpsRate', ossOpsRate[f])
                      printRate('mdsOpsRate', mdsOpsRate[f])

                   if not fsErr:
                      fsGangliaName = nameMap[f]
                      spoofIntoGanglia(g,      rRate[f], fsGangliaName + '_read_bytes',  'bytes/sec')
                      spoofIntoGanglia(g,      wRate[f], fsGangliaName + '_write_bytes', 'bytes/sec')
                      spoofIntoGanglia(g, ossOpsRate[f], fsGangliaName + '_oss_ops',     'ops/sec')
                      spoofIntoGanglia(g, mdsOpsRate[f], fsGangliaName + '_mds_ops',     'ops/sec')
                      if verbose:
                         print 'spoof into ganglia time', time.time() - t

             if verbose:
                print
             tOld = tLast
             first = 0
             if err:
                print >>sys.stderr, 'negative rate found. resetting all rates.'
                first = 1
             processed = 1
         continue

      # Handle inputs
      for s in readable:
         if s is server:
            # A "readable" server socket is ready to accept a connection
            connection, client_address = s.accept()
            print >>sys.stderr, 'new connection from', client_address
            connection.setblocking(0)
            inputs.append(connection)
            o[client_address] = {'size':-1}
            #print o.keys()
            # any new client appearing or old client disappearing will screw up rates
            first = 1
         else:
            data = s.recv(102400)
            c = s.getpeername()
            if data:
               # A readable client socket has data
               #print >>sys.stderr, 'received "%s" from %s, size %d' % (data, s.getpeername(), len(data))
               #print >>sys.stderr, 'from %s, size %d' % (s.getpeername(), len(data))

               #print 'non-server message', c
               if o[c]['size'] == -1: # new message
                  #print 'new msg'
                  if len(data) < 128:
                     print >>sys.stderr, 'short header. skipping', c, 'len', len(data)
                     continue
                  try:
                     # see client section for the fields in the data header
                     hashh = data[96:128]
                     if hashh != hashlib.md5(data[:96] + secretText).hexdigest():
                        print >>sys.stderr, 'corrupted header. skipping. hashes do not match', data[:96], hashh
                        continue
                     hashb = data[64:96]
                     n = int(data[:64].strip().split()[1])
                     #print 'header', data[:96], data[96:128], 'size', n
                     o[c]['size'] = n
                     o[c]['hash'] = hashb
                     o[c]['msg'] = data[128:]
                     o[c]['cnt'] = len(o[c]['msg'])
                  except:
                     print >>sys.stderr, 'thought it was a header, but failed'
                     pass  # something dodgy, skip
               else:
                  # more of an in-fight message
                  #print 'more. cnt', o[c]['cnt'], 'max', o[c]['size']
                  o[c]['msg'] += data
                  o[c]['cnt'] += len(data)

               if o[c]['cnt'] == o[c]['size']: # all there?
                  #print 'all done'
                  #print >>sys.stderr, 'got all from %s, size %d. checking & unpacking' % (s.getpeername(), o[c]['size'])
                  try:
                     # done!
                     # check the hash
                     hashb = hashlib.md5(o[c]['msg']).hexdigest()
                     if hashb != o[c]['hash']:
                        print >>sys.stderr, 'message corrupted. hash does not match. resetting'
                        zeroOss(o[c])
                        continue
                     # data is not corrupted. unpack
                     o[c]['data'] = cPickle.loads(o[c]['msg'])

                     # shimmy the datatype up from data dict to the oss level
                     # leaving just fs data in the (non-relay) data
                     o[c]['dataType'] = o[c]['data']['dataType']
                     del o[c]['data']['dataType']

                     t = time.time()
                     o[c]['time'] = t
                     tLast = t
                     processed = 0                        ## debug:
                     #for i in o[c]['data'].keys():
                     #   print i, len(o[c]['data'][i])
                     ## debug:
                     #j = 0
                     #for i in o.keys():
                     #   if 'data' in o[i].keys():
                     #      j += len(o[i]['data'].keys())
                     #   else:
                     #      print 'data not in oss keys of', i, 'keys', o[i].keys()
                     #print 'oss keys', len(o.keys()), 'ost keys', j
                  except:
                     print >>sys.stderr, 'corrupted data from', c
                  # put the message back into recv mode
                  # note that 'data' has not yet been processed so must be left alone
                  zeroOss(o[c])
               elif o[c]['cnt'] > o[c]['size']:
                  print >>sys.stderr, 'too much data. resetting', o[c]['cnt'], o[c]['size']
                  zeroOss(o[c])

            else:
               # Interpret empty result as closed connection
               print >>sys.stderr, 'closing', client_address, 'after reading no data.'
               # Stop listening for input on the connection
               inputs.remove(s)
               s.close()
               # delete all the data from that oss too
               del o[c]
               # any new client appearing or old client disappearing will screw up rates
               first = 1

      # Handle "exceptional conditions"
      for s in exceptional:
         print >>sys.stderr, 'handling exceptional condition for', s.getpeername()
         # Stop listening for input on the connection
         inputs.remove(s)
         s.close()
         # any new client appearing or old client disappearing will screw up rates
         first = 1

def syncToNextInterval( offset = 0 ):
   # sleep until the next interval
   t = time.time() % 60
   t += (60 + offset)   # optional time skew
   t %= 60
   i = int(t/dt)    # interval number
   sl = (i+1)*dt - t
   #print 'now', t, 'sleeping', sl , 'returning', i
   time.sleep(sl)
   return i, t

def connectSocket(sp):
   c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   try:
      c.connect(sp)
   except:
      print >>sys.stderr, 'could not connect to', sp
      c = None
   return c

def constructMessage(s):
   """construct header and body of message"""
   b = cPickle.dumps(s)
   hashb = hashlib.md5(b).hexdigest()

   # 128 byte header
   # this needs to be a fixed size as messages get aggregated
   #
   #   length
   #  in bytes    field
   #  --------   -------
   #     7       plain text 'header '
   #     N       message length in bytes ~= 6
   #  64-N-7     padding  (room left in here)
   #    32       hash of message body
   #    32       hash of all prev bytes of this header + contents of the shared secret file

   h = 'header %d' % len(b)
   h += ' '*(64-len(h))   # room in here for more fields if we need it
   h += hashb
   hashh = hashlib.md5(h + secretText).hexdigest()
   h += hashh
   #print 'h', h[:64], h[64:96], h[96:128], 'len(h)', len(h)

   return h, b

def clientCode( serverName, port, fsList ):
   while 1:
      i, now = syncToNextInterval()
      c = connectSocket( (serverName, port) )
      if c == None:
         time.sleep(5)
         continue

      while 1:
         t0 = time.time()
         s = {}
         s['dataType'] = 'direct'
         for f in fsList:
            s[f] = gatherStats(f)
         ## debug:
         ##print s
         #for o in s.keys():
         #   print o, len(s[o])

         h, b = constructMessage(s)
         try:
            c.send(h)
            c.send(b)
            #print 'sent', len(b)
         except:
            print >>sys.stderr, 'send of', len(h), len(b), 'failed'
            c.close()
            break

         iNew, now = syncToNextInterval()
         if iNew != (i+1)%clientSend or now - t0 > dt:
            print >>sys.stderr, 'collect took too long', now-t0, 'last interval', i, 'this interval', iNew
         i = iNew

def usage():
   print sys.argv[0] + '[-v|--verbose] [-d|--dryrun] [--secretfile file] [--port portnum] [--interface name] [server fsName1 [fsName2 ...]]'
   print '  server takes no args'
   print '  client needs a server name and one or more lustre filesystem names'
   print '  --verbose         - print summary of data sent to servers'
   print '  --dryrun          - do not send results to ganglia'
   print '  --secretfile file - specify an alternate shared secret file. default', secretFile
   print '  --port portnum    - tcp port num to send/recv on. default', port
   print '  --interface name  - make server listen on the interface that matches a hostname of "name".'
   print '                      default is to bind to the interface that matches gethostbyname'
   sys.exit(1)

def parseArgs( host ):
   global verbose, dryrun, secretFile, port, serverInterfaceName

   # parse optional args
   for v in ('-v', '--verbose'):
      if v in sys.argv:
         verbose = 1
         sys.argv.remove(v)
   for v in ('-d', '--dryrun'):
      if v in sys.argv:
         dryrun = 1
         sys.argv.remove(v)
   if '--secretfile' in sys.argv:
      v = sys.argv.index( '--secretfile' )
      assert( len(sys.argv) > v+1 )
      secretFile = sys.argv.pop(v+1)
      sys.argv.pop(v)
   if '--port' in sys.argv:
      v = sys.argv.index( '--port' )
      assert( len(sys.argv) > v+1 )
      port = int(sys.argv.pop(v+1))
      sys.argv.pop(v)
   if '--interface' in sys.argv:
      v = sys.argv.index( '--interface' )
      assert( len(sys.argv) > v+1 )
      serverInterfaceName = sys.argv.pop(v+1)
      sys.argv.pop(v)

   if len(sys.argv) == 1:
      return host, None # server takes no args
   if len(sys.argv) < 3:
      usage()
   return sys.argv[1], sys.argv[2:]

if __name__ == '__main__':
   host = socket.gethostname()
   serverName, fsList = parseArgs( host )
   readSecret()
   if host == serverName:
      if serverInterfaceName != None:
          serverName = serverInterfaceName
      serverCode( serverName, port ) # server recv code
   else:
      if serverInterfaceName != None:
         print 'error: --interface is an option for the server only'
         usage()
      clientCode( serverName, port, fsList ) # client send code
