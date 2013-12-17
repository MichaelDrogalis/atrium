# atrium

A Clojure library facilitating distributed heart beats for hot/warm stand-by fault tolerancy.
Requires a ZooKeeper server.

## Usage

```clojure
(let [ch (launch! {:host "127.0.0.1"                      ;;; ZooKeeper
                   :port 2181
                   :id (str (java.util.UUID/randomUUID))  ;;; Node identifier
                   :master-path "/master"                 ;;; ZK path to stash the master
                   :pulse-frequency 800                   ;;; Interval for master heart beat
                   :polling-frequency 1000})]             ;;; Interval to check for pulse
  (<!! ch) ;;; Unblocks when this node becomes the master
  (Thread/sleep 100000)) ;;; Program execution
```

## License

Copyright © 2013 Michael Drogalis

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
