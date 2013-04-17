# Phalanx is a text blocking HTTP service

## Building
* Use sbt (simple build tool)
* "sbt assembly" to generate one JAR in deploy/phalanx-server.jar (will also run tests)
```
$ sbt assembly
...Done packaging...
```
* Building requires wikifactory - will normally get it from our Maven repository

## Running
* Use "sbt run" in dev,
* or one JAR (above) in production "java -jar <jarname>"
```
$ cd deploy; java -jar phalanx-server.jar &
...Listening on port: 4666
```

## Tests
* use sbt test for unittests
* use doctests wrapper ( https://raw.github.com/Szumo/szumo-utils/master/doctests ) to run curl examples in this file (start server with deploy/phalanx-raw first)

## Web API URLs
* Parameters may be passed as query parameters or POST parameters. Both GET and POST may be used.
* Encode with UTF-8 encoding

### /
Responds with: `PHALANX ALIVE` to show that the server works.

```
$ curl --silent --noproxy localhost "http://localhost:4666/"
PHALANX ALIVE
```

### /check

Check if text matches any blocking rule (will stop on first that does). Required parameters:

* type - one of: content, summary, title, user, question_title, recent_questions, wiki_creation, cookie, email
* content - text to be checked
* lang - 2 letter lowercase language code (eg. en, de, ru, pl). "en" will be assumed if this is missing

Result will be either `ok\n` or `failure\n`

Examples:

```
$ curl --silent --noproxy localhost "http://localhost:4666/check?lang=en&type=content&content=hello"
ok
```

```
$ curl --silent --noproxy localhost "http://localhost:4666/check?lang=en&type=karamba&content=hello"
Unknown type parameter
```

```
$ curl --silent --noproxy localhost "http://localhost:4666/check?lang=en&type=content&content=pornhub.com"
failure
```


### /match
Paremeters are the same as for `/check`, but results will be a json list (potentially empty) of matching rule info.
Each rule info is a JSON dictionary with following keys: id, text, reason, caseSensitive, exact, regex, language, expires, authorId

```
$ curl --silent --noproxy localhost "http://localhost:4666/match?lang=en&type=content&content=hello"
[]
```

```
$ curl --silent --noproxy localhost "http://localhost:4666/match?lang=en&type=karamba&content=hello"
Unknown type parameter
```

```
$ curl --silent --noproxy localhost "http://localhost:4666/match?lang=en&type=content&content=pornhub.com"
[{"regex" : true, "expires" : "", "text" : "pornhub\\.com", "reason" : "SpamRegex initial import", "exact" : false, "caseSensitive" : false, "id" : 4009, "language" : "", "authorId" : 184532, "type" : 1}]
```


### /validate
Validates regular expression syntax from parameter "regex".
Result will be either `ok\n` or `failure\n` followed by error message

Examples:

```
$ curl --silent --noproxy localhost "http://localhost:4666/validate?regex=^alamakota$"
ok
```

```
$ curl --silent --noproxy localhost 'http://localhost:4666/validate?regex=^alama)))kota$'
failure
Unmatched closing ')' near index 5
^alama)))kota$
     ^
```

### /reload
Optional parameter: changed - comma seperated list of integer rule ids for partial reload
If not given, full reload will be done.
Notifies other nodes (node names should be in property com.wikia.phalanx.notifynodes)

```
$ curl --silent --noproxy localhost "http://localhost:4666/reload?changed=1,2,3"
ok
```

### /notify
Same as reload, but does not notify other nodes in the cluster.

```
$ curl --silent --noproxy localhost "http://localhost:4666/notify?changed=1,2,3"
ok
```


### /stats
Show some text info about currenty loaded rules.

Example:

`$ curl --silent --noproxy localhost "http://localhost:4666/stats"`

    Phalanx server version 0.20.2aa221581e609afe8e04cdd8ada8ef623701706b
    Next rule expire date: 2013-03-21 16:06:19 +0000
    NewRelic environment: not set
    Main worker threads: 4
    Max memory: 1.4 GiB
    Free memory: 1.1 GiB
    Total memory: 1.4 GiB
    Total time spent matching: 0.seconds
    Average time spent matching: unknown
    Matches done: 0
    User cache hits: 0
    Cache hit %: unknown
    Longest request time: unknown
    User cache: 0/8191

    email:
      CombinedRuleSystem with total 65 rules and 3 checkers
      Case insensitive (All langugages) [2 checkers]: exact set of 45 phrases, re2-regex (254 characters)
      Case sensitive (All langugages) [1 checkers]: exact set of 5 phrases
    wiki_creation:
      CombinedRuleSystem with total 313 rules and 6 checkers
      Case insensitive (All langugages) [2 checkers]: exact set of 67 phrases, re2-regex (2918 characters)
      Case insensitive (ru) [1 checkers]: exact set of 2 phrases
      Case sensitive (All langugages) [2 checkers]: exact set of 17 phrases, re2-regex (193 characters)
      Case sensitive (pl) [1 checkers]: exact set of 1 phrases
    question_title:
      CombinedRuleSystem with total 2117 rules and 9 checkers
      Case insensitive (All langugages) [2 checkers]: exact set of 61 phrases, re2-regex (12793 characters)
      Case insensitive (de) [1 checkers]: re2-regex (6237 characters)
      Case insensitive (en) [1 checkers]: re2-regex (5 characters)
      Case insensitive (fr) [1 checkers]: exact set of 1 phrases
      Case insensitive (ru) [1 checkers]: re2-regex (349 characters)
      Case sensitive (All langugages) [2 checkers]: exact set of 4 phrases, re2-regex (644 characters)
      Case sensitive (pl) [1 checkers]: exact set of 1 phrases
    cookie:
      CombinedRuleSystem with total 0 rules and 0 checkers
    content:
      CombinedRuleSystem with total 4591 rules and 9 checkers
      Case insensitive (All langugages) [2 checkers]: exact set of 204 phrases, re2-regex (165005 characters)
      Case insensitive (de) [1 checkers]: re2-regex (14 characters)
      Case insensitive (en) [1 checkers]: re2-regex (25 characters)
      Case insensitive (fr) [1 checkers]: re2-regex (17 characters)
      Case insensitive (ru) [1 checkers]: re2-regex (349 characters)
      Case sensitive (All langugages) [2 checkers]: exact set of 17 phrases, re2-regex (855 characters)
      Case sensitive (pl) [1 checkers]: exact set of 1 phrases
    title:
      CombinedRuleSystem with total 1244 rules and 7 checkers
      Case insensitive (All langugages) [2 checkers]: exact set of 662 phrases, re2-regex (12688 characters)
      Case insensitive (en) [1 checkers]: re2-regex (25 characters)
      Case insensitive (ru) [1 checkers]: re2-regex (349 characters)
      Case sensitive (All langugages) [2 checkers]: exact set of 24 phrases, re2-regex (556 characters)
      Case sensitive (pl) [1 checkers]: exact set of 1 phrases
    user:
      CombinedRuleSystem with total 9573 rules and 8 checkers
      Case insensitive (All langugages) [2 checkers]: exact set of 8470 phrases, re2-regex (11298 characters)
      Case insensitive (en) [1 checkers]: re2-regex (25 characters)
      Case insensitive (ru) [1 checkers]: re2-regex (349 characters)
      Case sensitive (All langugages) [2 checkers]: exact set of 212 phrases, re2-regex (839 characters)
      Case sensitive (en) [1 checkers]: exact set of 1 phrases
      Case sensitive (pl) [1 checkers]: exact set of 1 phrases
    summary:
      CombinedRuleSystem with total 989 rules and 8 checkers
      Case insensitive (All langugages) [2 checkers]: exact set of 179 phrases, re2-regex (19468 characters)
      Case insensitive (de) [1 checkers]: re2-regex (14 characters)
      Case insensitive (en) [1 checkers]: re2-regex (25 characters)
      Case insensitive (ru) [1 checkers]: re2-regex (349 characters)
      Case sensitive (All langugages) [2 checkers]: exact set of 14 phrases, re2-regex (1006 characters)
      Case sensitive (pl) [1 checkers]: exact set of 1 phrases
    recent_questions:
      CombinedRuleSystem with total 2837 rules and 5 checkers
      Case insensitive (All langugages) [2 checkers]: exact set of 36 phrases, re2-regex (12712 characters)
      Case insensitive (de) [1 checkers]: re2-regex (6308 characters)
      Case sensitive (All langugages) [2 checkers]: exact set of 4 phrases, re2-regex (88 characters)

### /stats/total
Show total matching time since last full reload

### /stats/avg
Show average matching time for cache misses since last full reload

### /stats/long
Shows 10 longest requests since last full reload

### /stats/checkers
Show potentially slow rules of 'user' and 'content' types (id: text)

### /view
Checks current information about a rule. Id must be passed with id parameter.
Result will be JSON with rule information.
Useful for debugging issues with reloading and matching.

Examples:

```
$ curl --silent --noproxy localhost "http://localhost:4666/view?id=100"
{"user" : {"regex" : true, "expires" : "", "text" : "Josh Gray", "reason" : "vandalism on swfanon", "exact" : false, "caseSensitive" : false, "id" : 100, "language" : "", "authorId" : 8245, "type" : 8}}
```

Kill server for doctests:

```
$ pkill -f "java -jar phalanx-server.jar"
ERRCODE=-15
```
