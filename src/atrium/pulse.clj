(ns atrium.pulse
  (:require [clojure.core.async :refer [chan thread <!! >!! timeout]]
            [zookeeper :as zk]
            [dire.core :as d])
  (:import [org.apache.zookeeper KeeperException$BadVersionException]
           [org.apache.zookeeper KeeperException$NoNodeException]))

(defn serialize-edn [x]
  (.getBytes (pr-str x)))

(defn deserialize-edn [x]
  (read-string (String. x "UTF-8")))

(def pulse-path "/atrium")

(defn pulse-node [id]
  (str pulse-path "/" id))

(defn create-pulse-path [client]
  (zk/create client pulse-path :persistent? true))

(defn create-pulse-node [client id]
  (zk/create client (pulse-node id))
  (let [version (:version (zk/exists client (pulse-node id)))]
    (zk/set-data client (pulse-node id) (serialize-edn {:pulse 0}) version)))

(defn create-master-node [client node]
  (zk/create client node :persistent? true))

(defn monitor-for-failure
  ([master-id config client] (monitor-for-failure master-id config client nil 0))
  ([master-id config client prev-ver missed-beats]
     (if (>= missed-beats 2)
       (throw (ex-info "Master pulse stopped" {:master master-id}))
       (do (<!! (timeout (:polling-frequency config)))
           (let [payload (zk/data client (pulse-node master-id))
                 version (:version (:stat payload))]
             (if (not= version prev-ver)
               (do (println "Saw the heart beat at version" version)
                   (recur master-id config client version 0))
               (do (println "Saw a missed heart beat at version" version)
                   (recur master-id config client version (inc missed-beats)))))))))

(defn stand-by [config client payload ch]
  (let [master-id (:id (deserialize-edn (:data payload)))]
    (monitor-for-failure master-id config client)))

(defn trigger-rendezvous [e config client payload ch]
  (>!! ch (:version (:stat payload))))

(d/with-handler! #'stand-by
  java.lang.NullPointerException
  trigger-rendezvous)

(d/with-handler! #'stand-by
  clojure.lang.ExceptionInfo
  trigger-rendezvous)

(d/with-handler! #'stand-by
  KeeperException$NoNodeException
  trigger-rendezvous)

(defn elect-self [client {:keys [master-path id]} version]
  (let [node (zk/data client master-path)]
    (zk/set-data client master-path (serialize-edn {:id id}) version)
    true))

(d/with-handler! #'elect-self
  KeeperException$BadVersionException
  (fn [e client config version]
    (println (:id config) "was beat by another node in becoming the master")
    false))

(defn beat [client id master {:keys [data stat] :as pulse-data}]
  (zk/set-data client (pulse-node id) data (:version stat)))

(defn heart-beat-process [client {:keys [id master-path pulse-frequency]}]
  (thread
   (while true
     (let [master (zk/data client master-path)
           {:keys [data stat]} (zk/data client (pulse-node id))]
       (beat client id master (zk/data client (pulse-node id))))
     (<!! (timeout pulse-frequency)))))

(defn launch! [{:keys [host port id master-path] :as config}]
  (let [user-ch (chan)
        client (zk/connect (str host ":" port))]
    (create-pulse-path client)
    (create-master-node client master-path)
    (create-pulse-node client id)
    (thread
     (loop [rendezvous (chan)]
       (let [payload (zk/data client master-path)]
         (thread (stand-by config client payload rendezvous))
         (let [version (<!! rendezvous)]
           (if (elect-self client config version)
             (do (heart-beat-process client config)
                 (>!! user-ch true))
             (recur (chan)))))))
    user-ch))

(d/with-precondition! #'beat
  :still-master
  (fn [client id master {:keys [data stat] :as pulse-data}]
    (= (:id (deserialize-edn (:data master))) id)))

(d/with-handler! #'beat
  {:precondition :still-master}
  (fn [e client id master {:keys [data stat] :as pulse-data}]
    (println id "is not the master anymore. Dying")))

(d/with-pre-hook! #'beat
  (fn [client id master {:keys [data stat] :as pulse-data}]
    (println id "heart beats")))

(d/with-pre-hook! #'stand-by
  (fn [config client payload ch]
    (println "On stand-by")))

(d/with-pre-hook! #'elect-self
  (fn [client {:keys [id master-path]} version]
    (println id "is attempting to become the master")))

(d/with-post-hook! #'elect-self
  (fn [_] (println "Taking over as the master")))

