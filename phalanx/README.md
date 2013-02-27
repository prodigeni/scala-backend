# Phalanx is a text blocking HTTP service

## Building
* Use sbt (simple build tool)
* "sbt assembly" to generate one JAR
* Building requires wikifactory - will normally get it from our Maven repository

## Running
* Use "sbt run" in dev,
* or one JAR (above) in production "java -jar <jarname>"

## Tests
* use sbt test for unittests
* use doctests wrapper ( https://raw.github.com/Szumo/szumo-utils/master/doctests ) to run curl examples in this file (start server with deploy/phalanx-raw first)

## Web API URLs
* Parameters may be passed as query parameters or POST parameters. Both GET and POST may be used.
* Encode with UTF-8 encoding

### /
Responds with: `PHALANX ALIVE` to show that the server works.

```
$ curl --silent "http://localhost:4666/"
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
$ curl --silent "http://localhost:4666/check?lang=en&type=content&content=hello"
ok
```

```
$ curl --silent "http://localhost:4666/check?lang=en&type=karamba&content=hello"
Unknown type parameter
```

```
$ curl --silent "http://localhost:4666/check?lang=en&type=content&content=pornhub.com"
failure
```


### /match
Paremeters are the same as for `/check`, but results will be a json list (potentially empty) of matching rule info.
Each rule info is a JSON dictionary with following keys: id, text, reason, caseSensitive, exact, regex, language, expires, authorId

```
$ curl --silent "http://localhost:4666/match?lang=en&type=content&content=hello"
[]
```

```
$ curl --silent "http://localhost:4666/match?lang=en&type=karamba&content=hello"
Unknown type parameter
```

```
$ curl --silent "http://localhost:4666/match?lang=en&type=content&content=pornhub.com"
[{"regex" : true, "expires" : "", "text" : "pornhub\\.com", "reason" : "SpamRegex initial import", "exact" : false, "caseSensitive" : false, "id" : 4009, "language" : "", "authorId" : 184532, "type" : 1}]
```


### /validate
Validates regular expression syntax from parameter "regex".
Result will be either `ok\n` or `failure\n`

Examples:

```
$ curl --silent "http://localhost:4666/validate?regex=^alamakota$"
ok
```

```
$ curl --silent "http://localhost:4666/validate?regex=^alama(((kota$"
failure
```

### /reload
Optional parameter: changed - comma seperated list of integer rule ids for partial reload
If not given, full reload will be done.
Notifies other nodes (node names should be in property com.wikia.phalanx.notifynodes)

```
$ curl --silent "http://localhost:4666/reload?changed=1,2,3"
ok
```

### /notify
Same as reload, but does not notify other nodes in the cluster.

```
$ curl --silent "http://localhost:4666/notify?changed=1,2,3"
ok
```


### /stats
Show some text info about currenty loaded rules.

Example:

`$ curl --silent "http://localhost:4666/stats"`

    email:
      CombinedRuleSystem with total 75 rules and 3 checkers
      Case insensitive (All langugages) : exact set of 56 phrases, regex (234 characters)
      Case sensitive (All langugages) : exact set of 5 phrases

    wiki_creation:
      CombinedRuleSystem with total 326 rules and 5 checkers
      Case insensitive (All langugages) : exact set of 69 phrases, regex (3169 characters)
      Case insensitive (ru) : exact set of 2 phrases
      Case sensitive (All langugages) : exact set of 16 phrases, regex (189 characters)

    question_title:
      CombinedRuleSystem with total 2099 rules and 8 checkers
      Case insensitive (All langugages) : exact set of 60 phrases, regex (14924 characters)
      Case insensitive (de) : regex (8705 characters)
      Case insensitive (en) : regex (9 characters)
      Case insensitive (fr) : exact set of 1 phrases
      Case insensitive (ru) : regex (347 characters)
      Case sensitive (All langugages) : exact set of 4 phrases, regex (606 characters)

    cookie:
      CombinedRuleSystem with total 0 rules and 0 checkers

    content:
      CombinedRuleSystem with total 4635 rules and 8 checkers
      Case insensitive (All langugages) : exact set of 216 phrases, regex (160112 characters)
      Case insensitive (de) : regex (12 characters)
      Case insensitive (en) : regex (23 characters)
      Case insensitive (fr) : regex (21 characters)
      Case insensitive (ru) : regex (347 characters)
      Case sensitive (All langugages) : exact set of 17 phrases, regex (851 characters)

    title:
      CombinedRuleSystem with total 1270 rules and 6 checkers
      Case insensitive (All langugages) : exact set of 671 phrases, regex (13397 characters)
      Case insensitive (en) : regex (53 characters)
      Case insensitive (ru) : regex (347 characters)
      Case sensitive (All langugages) : exact set of 24 phrases, regex (504 characters)

    user:
      CombinedRuleSystem with total 14817 rules and 7 checkers
      Case insensitive (All langugages) : exact set of 11173 phrases, regex (11902 characters)
      Case insensitive (en) : regex (23 characters)
      Case insensitive (ru) : regex (347 characters)
      Case sensitive (All langugages) : exact set of 2596 phrases, regex (1057 characters)
      Case sensitive (en) : exact set of 1 phrases

    summary:
      CombinedRuleSystem with total 1033 rules and 7 checkers
      Case insensitive (All langugages) : exact set of 189 phrases, regex (20199 characters)
      Case insensitive (de) : regex (12 characters)
      Case insensitive (en) : regex (23 characters)
      Case insensitive (ru) : regex (347 characters)
      Case sensitive (All langugages) : exact set of 14 phrases, regex (1002 characters)

    recent_questions:
      CombinedRuleSystem with total 2829 rules and 5 checkers
      Case insensitive (All langugages) : exact set of 36 phrases, regex (15758 characters)
      Case insensitive (de) : regex (8820 characters)
      Case sensitive (All langugages) : exact set of 4 phrases, regex (80 characters)

### /view
Checks current information about a rule. Id must be passed with id parameter.
Result will be JSON with rule information.
Useful for debugging issues with reloading and matching.

Examples:

```
$ curl --silent "http://localhost:4666/view?id=100"
{"user" : {"regex" : true, "expires" : "", "text" : "Josh Gray", "reason" : "vandalism on swfanon", "exact" : false, "caseSensitive" : false, "id" : 100, "language" : "", "authorId" : 8245, "type" : 8}}
```
