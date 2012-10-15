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
#   /proc/fs/lustre/mds/data-MDT0000/exports/10.1.14.1@o2ib/stats

import os, socket, select, sys, cPickle, time, subprocess, md5

# these should be command line or config file args:
port = 8022
clientSend = 3  # number of gathers per minute on clients

# could generalise this to multiple clusters?
nameMap = { 'data':'vu_short',
            'apps':'vu_apps',
            'home':'vu_home',
          'images':'vu_images',
          'system':'vu_system' }

# lnets local to each cluster 
localLnets = { 'vu-pbs':'o2ib', 'xepbs':'o2ib2', 'dccpbs':'tcp102' }

# relaying servers send summed data to various other server instances.
# specify which servers do relaying, and which lnet's get send to which servers
relay = { 'alkindi': [ 'vu-pbs', 'xepbs', 'dccpbs' ] }

statsDir = { 'oss':'/proc/fs/lustre/obdfilter', 'mds':'/proc/fs/lustre/mds' }
verbose = 0
dryrun = 0
# shared secret between client and servers. only readable by root
secretFile = '/root/.lustreHarvest.secret'

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
   if len(s) == 0:  # no data for this fs
      return {}
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
            ds = 0
         rates[h] = float(ds)/deltat
      return rates

   # if clients have changed (always added, never removed?) then do the full much slower loop
   for h in s.keys():
      if h in sOld.keys():
         ds = s[h] - sOld[h]
         if ds < 0:
            print >>sys.stderr, 'clients changed. negative rate', h, ds, s[h], sOld[h]
            ds = 0
         rates[h] = float(ds)/deltat
      else:
         rates[h] = 0.0
   return rates

def readStatsFile(fn):
   l = open(fn,'r').readlines()
   # turn into a dict
   i = {}
   for ll in l:
      j = ll.split()
      i[j[0]] = j[1:]
   # ignore snapshot_time and ping, but harvest and sum all the other [reqs] and call them iops
   ops = 0
   for n,j in i.iteritems():
      if len(j) < 3 or n in ( 'read_bytes', 'write_bytes', 'snapshot_time', 'ping' ):
         continue
      if j[2] == '[reqs]':
         ops += int(j[0])
   # do read and write
   r = 0
   if 'read_bytes' in i.keys():  # only populated in stats file if used
      r = int(i['read_bytes'][5])
   w = 0
   if 'write_bytes' in i.keys():
      w = int(i['write_bytes'][5])
   return ( r, w, ops )

def gatherStats(fs):
   s = {}
   osts = []
   # handle both mds and oss
   for machType, ld in statsDir.iteritems():
      try:
         dirs = os.listdir(ld)
      except:
         continue
      # find osts
      for d in dirs:
         if d[:len(fs)] == fs:
            osts.append(d)
      for o in osts:
         s[o] = {}
         s[o]['type'] = machType   # oss or mds data
         ostDir = ld + '/' + o + '/exports'
         # loop over all clients
         for c in os.listdir(ostDir):
            #print c
            try:
               s[o][c] = readStatsFile(ostDir + '/' + c + '/stats')
               #print s[o][c]
            except:
               pass
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
   t = time.time()

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
      for f in o[oss]['data'].keys():  # filesystems
         for ost in o[oss]['data'][f].keys():
            c.extend(o[oss]['data'][f][ost].keys())
   c.sort()
   c = uniq(c)
   c.remove('type')
   if verbose:
      #print 'clients', len(c)
      print 'oss', len(o.keys()), 'ost', Nost, 'clients', len(c), 'filesystems', fss
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
                  # debug
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
                  # debug
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


def doRelay(rs, host, port, data):
   # bundle all data up into a message of a new type to send to other clusters
   if host not in relay.keys():
      return

   # setup connections to the hosts we are relaying to
   for hnin relay[host]:
      if hn not in rs.keys() or rs[hn] == None:
         rs[hn] = connectSocket((hn, port))
         print >>sys.stderr, 'setting up new relay connection to', (hn,port)

   # construct message
   #   ... for now, send all data, regardless of lnet
   s = {}
   s['dataType'] = 'relay'
   s['data'] = data

   for hn in relay[host]:
      h, b = contructMessage(s)
      c = rs[hn]
      if c == None:
         continue
      try:
         c.send(h)
         c.send(b)
         #print 'sent', len(b)
      except:
         print >>sys.stderr, 'relay send of', len(h), len(b), 'failed'
         c.close()
         rs[hn] = None

   return rs


# server relay code:
#  - as recv message, tag data as being of different type
#    but leave it in o[c] anyway for convenience of message handling
#  - skip processing of this data in sumDataToClients()
#  - call mergeWithPreSummedData() to merge data from remote
#    clients with regular clients
#  - can sum per oss data and send to /g/data ganglia as well ??

# notes:
#  - time skew and delay in data is necessary as need to recv, delay, sum and then send summed data
#    ie. relaying cluster needs to run with an offset of ~5s before the others
#  - simple way to write it is to relay all summed data to all clusters ie. unecessarily large transmits,
#    but this is necessary in the 'zero knowledge of endpoints' relayer model



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
   g = gmetric.Gmetric( '239.2.11.71', 8649, 'multicast' )
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
             r, w, ossOps, mdsOps, fss = sumDataToClients(o, t)
             # relay some of the summed data to other server instances
             rs = doRelay(rs, serverName, port, (r, w, ossOps, mdsOps, fss))
             # remove data fields to avoid re-processing data from stopped oss's. not necessary??
             removeProcessedData(o)
             if fss != fssOld:
                first = 1
             if not first:
                if verbose:
                   print 'rate dt', tLast - tOld
                for f in fss:  # loop over each fs
                   if verbose:
                      print 'fs', f
                   t = time.time()
                   rRate[f] = computeRates( rOld[f], r[f], tOld, tLast )
                   wRate[f] = computeRates( wOld[f], w[f], tOld, tLast )
                   ossOpsRate[f] = computeRates( ossOpsOld[f], ossOps[f], tOld, tLast )
                   mdsOpsRate[f] = computeRates( mdsOpsOld[f], mdsOps[f], tOld, tLast )
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
            o[str(client_address)] = {'size':-1}
            # any new client appearing or old client disappearing will screw up rates
            first = 1
         else:
            data = s.recv(102400)
            if data:
               # A readable client socket has data
               #print >>sys.stderr, 'received "%s" from %s, size %d' % (data, s.getpeername(), len(data))
               #print >>sys.stderr, 'from %s, size %d' % (s.getpeername(), len(data))

               c = str(s.getpeername())
               if o[c]['size'] == -1: # new message
                  #print 'new msg'
                  if len(data) < 128:
                     print >>sys.stderr, 'short header. skipping', c, 'len', len(data)
                     continue
                  try:
                     # see client section for the fields in the data header
                     hashh = data[96:128]
                     if hashh != md5.new(data[:96] + secretText).hexdigest():
                        print >>sys.stderr, 'corrupted header. skipping. hashes do not match'
                        continue
                     hashb = data[64:96]
                     n = int(data[:64].strip().split()[1])
                     #print 'header', data[:128], 'size', n
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
                     try:
                        # done!
                        # check the hash
                        hashb = md5.new(o[c]['msg']).hexdigest()
                        if hashb != o[c]['hash']:
                           print >>sys.stderr, 'message corrupted. hash does not match. resetting'
                           zeroOss(o[c])
                           continue
                        # data is not corrupted. unpack
                        o[c]['data'] = cPickle.loads(o[c]['msg'])
                        t = time.time()
                        o[c]['time'] = t
                        tLast = t
                        processed = 0
                        ## debug:
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
                     print >>sys.stderr, 'too much data. resetting'
                     zeroOss(o[c])

            else:
               # Interpret empty result as closed connection
               print >>sys.stderr, 'closing', client_address, 'after reading no data'
               # Stop listening for input on the connection
               inputs.remove(s)
               s.close()
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
   hashb = md5.new(b).hexdigest()

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
   hashh = md5.new(h + secretText).hexdigest()
   h += hashh
   #print 'header len', len(h)

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

         h, b = contructMessage(s)
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
   print sys.argv[0] + '[-v|--verbose] [-d|--dryrun] [--secretfile file] [server fsName1 [fsName2 ...]]'
   print '  server takes no args'
   print '  client needs a server name and one or more lustre filesystem names'
   print '  --verbose         - print summary of data sent to servers'
   print '  --dryrun          - do not send results to ganglia'
   print '  --secretfile file - specify an alternate shared secret file. default', secretFile
   sys.exit(1)

def parseArgs( host ):
   global verbose, dryrun, secretFile

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
      serverCode( serverName, port ) # server recv code
   else:
      clientCode( serverName, port, fsList ) # client send code
