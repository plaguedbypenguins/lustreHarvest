#!/usr/bin/env python

# slurp fs stats from ganglia gmond xml to check spoofing is working
#  (c) rjh - Fri Oct  5 18:16:27 EST 2012
# licensed under the GPL v3 or later

import sys

def read(gmondHost):
   import socket
   sock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
   try:
      sock.connect( (gmondHost, 8649) )
   except:
      return

   xmlData = ''
   while 1:
      data = sock.recv(102400)
      if not data:
         break
      xmlData += data
   sock.shutdown(2)

   return xmlData.split('\n')

def search(x, name):
   import re
   r = re.compile( '<HOST NAME|' + name )
   h = re.compile( '<HOST NAME' )
   rr = []
   hostline=0
   prev = ''
   ll = ''
   for l in x:
      #print 'l', l
      if r.search(l):
         if h.search(l):
            #if hostline:  # error, 2 hosts in a row, but we don't care as the 2nd is right.
            #   print 'err, 2 hostlines in a row. this', l, 'prev', ll
            #   continue
            hostline=1
            ll = l
         else:
            if not hostline:  # error, 2 metric lines in a row
               print 'err, 2 metric in a row. this', l, 'prev', prev
               continue
            hostline=0
            prev = l
            rr.append(ll + l)
   return rr

def reduce(r):
   d = []
   s = 0
   for l in r:
      a = l.split('"')
      n = float(a[21])
      d.append((n,a[1]))
      s += n
   return d, s

gig = 1024.0*1024.0*1024.0
meg = 1024.0*1024.0
kay = 1024.0

def scalarToScaled( d ):
    if d > 3000000000:
        return ( d/gig, ' GB/s' )
    elif d > 1500000:
        return ( d/meg, ' MB/s' )
    elif d > 1500:
        return ( d/kay, ' KB/s' )
    return ( d, ' B/s' )

def printBytes(b, highlight, units):
   if units:
      v, u = scalarToScaled(b)
   else:
      v = b
   if highlight:
      s = bold + '%.0f' % v + normal
   else:
      s = '%.0f' % v
   if units:
      s += u
   return s

def printTop(d, b):
   thresh=50
   l = 0
   for i in range(1,11):
      r, n = d[-i]
      if r > thresh:
         l = max(l, len(n))
   for i in range(1,11):
      r, n = d[-i]
      if r > thresh:
         print n + ' '*(l - len(n)), printBytes(r, 0, b)

def parseArgs():
   hn = 'localhost'
   if len(sys.argv) > 1:
      if sys.argv[1][0] == '-': # -anything is help
         usage()
      hn = sys.argv[1]
   print 'using gmond data from', hn
   return hn

def usage():
   print sys.argv[0], '[--help] [host]'
   sys.exit(1)

if __name__ == '__main__':
   from lustreHarvest import nameMap

   hn = parseArgs()

   # read from gmond on the local node
   x = read(hn)

   # see what we have in ganglia...
   m = []
   for i,j in nameMap.iteritems():
      for k in ( '_mds_ops', '_oss_ops', '_read_bytes', '_write_bytes' ):
         m.append( j + k )
   for op in m:
      r = search(x, op)   
      if not len(r):
         continue
      d, sum = reduce(r)

      b = 0
      if '_ops' in op:
         print op, 'total', printBytes(sum, 0, b), 'ops/s'
      else:
         b = 1
         print op, 'total', printBytes(sum, 0, b)

      d.sort()
      printTop(d, b)
      print
