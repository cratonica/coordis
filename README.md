coordis
=======

A task coordinator for distributed systems using [Redis](http://redis.io/) as a mediator.

This Go implementation uses [Redigo](http://github.com/garyburd/redigo) to connect to Redis.

Documentation: http://godoc.org/github.com/cratonica/coordis

Testing
-------
To run the tests, make sure Redis is running locally on the default port (6379).
Note that it will trample any keys in the default database matching \_go\_coordis\_test\* 

Contributing
------------
Contributions are welcome, it would be great to have support for multiple
tasks sharing the same prerequisite without complicating the API and
retaining atomicity.

The protocol is fairly simple and could easily be implemented in other programming languages,
please let me know if you create such a project so I can link to it.

