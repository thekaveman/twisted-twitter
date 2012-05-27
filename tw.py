import twisted, urllib, re, datetime, os, ast
import twisted.web.client as webClient
import twisted.internet.defer as defer
from twisted.internet import reactor, protocol
from twisted.python import log

NEW_LN = os.linesep

class TWProtocol(twisted.protocols.basic.LineOnlyReceiver):
    
    def connectionMade(self):
        log.msg("Connection made", system="TWProtocol")
        
    def connectionLost(self, reason):
        log.msg("Connection lost: { %s }" % reason, system="TWProtocol")

    def lineReceived(self, line):
        timeStamp = datetime.datetime.now()
        log.msg("Line received: { %s } at { %s }" 
            % (line, timeStamp.strftime("%m/%d/%Y %H:%M:%S")), system="TWProtocol")
        #processLine returns a deferred
        d = self.processLine(line, timeStamp)
        #when processLine completes, send respnse to client
        d.addCallback(self.clientResponseHandler)

    def processLine(self, line, timeStamp):
       log.msg("Processing line: { %s }" % line, system="TWProtocol")
       
       if line:
            try:
                cmd, data = line.split(None, 1)
            except:
                return self.errorHandler("? invalid query: %s" % line)
                
            #look for a handler for the specified cmd
            thunk = getattr(self, "cmd_%s" % (cmd.upper(),), None)
            #if a handler is found, call it with params and return its value
            if thunk:
                return thunk(data, timeStamp)
            else:
                log.msg("Invalid command: { %s } in: { %s }" 
                    % (cmd, line), system="TWProtocol")
                return defer.succeed("? %s" % line)
       else:
            return self.errorHandler("? empty query")

    def cmd_AT(self, data, timeStamp):
        log.msg("Calling cmd_AT with: { %s }" % data, system="TWProtocol")
        
        try:
            client, gps = data.split()
        except:
            return self.errorHandler("? invalid query: %s" % data)
        
        if client in self.factory.clients:
            log.msg("Updating client: { %s }" % client, system="TWProtocol")
        else:
            log.msg("Adding client: { %s }" % client, system="TWProtocol")

        self.factory.clients[client] = [timeStamp, gps]
        return defer.succeed("AT %s %s" % (self.transport.getHost(), data))

    def cmd_WHATSAT(self, data, timeStamp):
        log.msg("Calling cmd_WHATSAT with: { %s }" % data, system="TWProtocol")
        
        try:
            client, rad, limit, query = data.split(None, 3)
        except:
            return self.errorHandler("? invalid query: %s" % data)
            
        if client in self.factory.clients:
            ts, gps = self.factory.clients[client]
            log.msg("Querying client: { %s } with: { %s %s %s } at: { %s }"
                % (client, query, rad, limit, gps), system="TWProtocol")
                
            d = self.factory.twitterQuery(query, limit, gps, rad)
            d.addCallback(self.webResponseHandler)
            return d
        else:
            log.msg("Failed query: { %s } on unknown client: { %s }"
                % (data, client), system="TWProtocol")
            return defer.succeed("Client: %s doesn't exist" % client)

    def webResponseHandler(self, response):
        #wrap nulls in quotes so they are eval'd as strings
        resp = response.replace("null", "'null'")
        log.msg("Got response: %s" % resp, system="TWProtocol")                
        #resp should be a JSON-encoded string (i.e. a dictionary)
        #converts string to dictionary
        d = ast.literal_eval(resp)
        rs = d["results"]
        output = []
        output.append("%sQuery: %s (%d results)%s"
            % (NEW_LN, d["query"], len(rs), NEW_LN*2))
        for r in rs:
            output.append("TS: %s%s" % (r["created_at"], NEW_LN))
            output.append("Geo: %s%s" % (r["geo"], NEW_LN))
            output.append("UserID: %s%s" % (r["from_user_id"], NEW_LN))
            output.append("UserName: %s%s" % (r["from_user"], NEW_LN))
            output.append("Text: %s%s" % (r["text"].replace("\\", ""), NEW_LN*2))
            
        return "".join(output)

    def clientResponseHandler(self, response):
        log.msg("Responding: %s" % response, system="TWProtocol")
        self.sendLine("%s" % response)
    
    def errorHandler(self, msg):
        return defer.succeed("Error: %s" % msg)

class TWFactory(twisted.internet.protocol.ServerFactory):
    protocol = TWProtocol
    twGET = "http://search.twitter.com/search.json?%s"

    def __init__(self):
        self.clients = {}

    def twitterQuery(self, q, rpp, gps, rad):
        geo = self.geocodeQS(gps, rad)
        log.msg("GET twitter JSON: q=%s rpp=%s geo=%s" 
            % (q, rpp, geo), system="TWFactory")

        params = urllib.urlencode([("q", q.replace("[", "").replace("]", "")), 
                                ("rpp", rpp), 
                                ("geocode", geo)],
                                ("result_type", "recent"))
        url = (self.twGET % params)
        return webClient.getPage(url)

    def geocodeQS(self, gps, rad):
        log.msg("Geocoding gps=%s, rad=%s" % (gps, rad), system="TWFactory")
        m = re.match('([\+|-]\d+\.?\d*)([\+|-]\d+\.?\d*)', gps)
        lat, lng = m.group(1), m.group(2)
        return "%f,%f,%skm" % (float(lat), float(lng), rad)

#open log in append mode, use "w" to open in (over)write mode
LOG_PATH = "C:\\temp\\TW.log" if os.name == "nt" else "~/TW.log"
log.startLogging(open(LOG_PATH, "a"))
#start the main twisted (reactor) loop on localhost 1234
reactor.listenTCP(1234, TWFactory())
reactor.run()