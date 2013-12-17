# atrium

A Clojure library facilitating distributed heart beats for hot/warm stand-by fault tolerancy.
Requires a ZooKeeper server and core.async.

## Usage

Node process:

```clojure
(let [ch (launch!
          {:host "127.0.0.1"                       ;;; ZooKeeper server
           :port 2181                              
           :id (str (java.util.UUID/randomUUID))   ;;; Node identifier
           :master-path "/master"                  ;;; ZK path to stash the master
           :pulse-frequency 800                    ;;; Interval for master heart beat
           :polling-frequency 1000})]              ;;; Interval to check for pulse
  (<!! ch)
  (Thread/sleep 100000)) ;;; Program execution
```

Master observation:

```clojure
(master {:host "127.0.0.1"
         :port 2181
         :master-path "/master"}) ;;; => "c41a257d-cf49-4559-bc1d-6140461ad31c"
```

## License

Copyright © 2013 Michael Drogalis

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
