commons-httpclient-failover
---------------------------

This projet implements a wrapper around Apache commons-httpclient 3.X to
provide automatic failover and load-balancing between several remote hosts
providing the same service.

It is designed to gracefully handle the cases where:
* One of the remote hosts is going down as planned
  (through a "is alive" mechanism)
* Some remote hosts are down
* Some remote hosts are "hanged", maybe accepting connections, but not
  answering
